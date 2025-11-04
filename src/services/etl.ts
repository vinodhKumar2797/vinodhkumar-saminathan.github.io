import { supabase } from '../lib/supabase';
import { RawLinkedInProfile, ETLRun, ProfileChange } from '../types/linkedin';
import { ProfileValidator } from './validation';
import { HashService } from './hash';
import { auth } from '../lib/auth';

export class ETLService {
  private validator = new ProfileValidator();
  private hashService = new HashService();

  async startETLRun(runType: 'full' | 'incremental'): Promise<string> {
    const user = await auth.getCurrentUser();
    if (!user) throw new Error('Not authenticated');

    const { data, error } = await supabase
      .from('etl_runs')
      .insert({
        user_id: user.id,
        run_type: runType,
        status: 'running',
        started_at: new Date().toISOString(),
      })
      .select()
      .single();

    if (error) throw error;
    return data.id;
  }

  async completeETLRun(
    runId: string,
    stats: {
      profiles_processed: number;
      profiles_added: number;
      profiles_updated: number;
      profiles_unchanged: number;
      images_processed: number;
      validation_failures: number;
    }
  ): Promise<void> {
    const { error } = await supabase
      .from('etl_runs')
      .update({
        status: 'completed',
        completed_at: new Date().toISOString(),
        ...stats,
      })
      .eq('id', runId);

    if (error) throw error;
  }

  async failETLRun(runId: string, errorMessage: string): Promise<void> {
    const { error } = await supabase
      .from('etl_runs')
      .update({
        status: 'failed',
        completed_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('id', runId);

    if (error) throw error;
  }

  async processProfile(
    rawProfile: RawLinkedInProfile,
    runId: string
  ): Promise<'added' | 'updated' | 'unchanged'> {
    const validationErrors = this.validator.validate(rawProfile);
    const validationStatus = this.validator.getValidationStatus(validationErrors);
    const dataHash = await this.hashService.generateProfileHash(rawProfile);

    const { data: existingProfile } = await supabase
      .from('linkedin_profiles')
      .select('*')
      .eq('linkedin_id', rawProfile.linkedin_id)
      .maybeSingle();

    const profileData = {
      linkedin_id: rawProfile.linkedin_id,
      full_name: rawProfile.full_name,
      headline: rawProfile.headline || '',
      location: rawProfile.location || '',
      summary: rawProfile.summary || '',
      experience: rawProfile.experience || [],
      education: rawProfile.education || [],
      skills: rawProfile.skills || [],
      connections_count: rawProfile.connections_count || 0,
      profile_url: rawProfile.profile_url,
      data_hash: dataHash,
      validation_status: validationStatus,
      validation_errors: validationErrors,
      last_validated_at: new Date().toISOString(),
    };

    if (!existingProfile) {
      const user = await auth.getCurrentUser();
      if (!user) throw new Error('Not authenticated');

      const { data: newProfile, error } = await supabase
        .from('linkedin_profiles')
        .insert({
          ...profileData,
          user_id: user.id,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        })
        .select()
        .single();

      if (error) throw error;

      if (rawProfile.profile_image_url) {
        await this.processImage(
          newProfile.id,
          'profile_photo',
          rawProfile.profile_image_url
        );
      }

      if (rawProfile.banner_image_url) {
        await this.processImage(
          newProfile.id,
          'banner',
          rawProfile.banner_image_url
        );
      }

      return 'added';
    }

    if (existingProfile.data_hash === dataHash) {
      await supabase
        .from('linkedin_profiles')
        .update({
          last_validated_at: new Date().toISOString(),
          validation_status: validationStatus,
          validation_errors: validationErrors,
        })
        .eq('id', existingProfile.id);

      return 'unchanged';
    }

    await this.trackChanges(existingProfile, profileData, runId);

    const { error } = await supabase
      .from('linkedin_profiles')
      .update({
        ...profileData,
        updated_at: new Date().toISOString(),
      })
      .eq('id', existingProfile.id);

    if (error) throw error;

    if (rawProfile.profile_image_url) {
      await this.processImage(
        existingProfile.id,
        'profile_photo',
        rawProfile.profile_image_url
      );
    }

    if (rawProfile.banner_image_url) {
      await this.processImage(
        existingProfile.id,
        'banner',
        rawProfile.banner_image_url
      );
    }

    return 'updated';
  }

  private async trackChanges(
    oldProfile: Record<string, unknown>,
    newProfile: Record<string, unknown>,
    runId: string
  ): Promise<void> {
    const changes: ProfileChange[] = [];
    const fieldsToTrack = [
      'full_name',
      'headline',
      'location',
      'summary',
      'connections_count',
    ];

    for (const field of fieldsToTrack) {
      const oldValue = oldProfile[field];
      const newValue = newProfile[field];

      if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
        changes.push({
          id: crypto.randomUUID(),
          profile_id: oldProfile.id as string,
          etl_run_id: runId,
          field_name: field,
          old_value: String(oldValue),
          new_value: String(newValue),
          changed_at: new Date().toISOString(),
        });
      }
    }

    if (changes.length > 0) {
      await supabase.from('profile_change_history').insert(changes);
    }
  }

  private async processImage(
    profileId: string,
    imageType: 'profile_photo' | 'banner',
    imageUrl: string
  ): Promise<void> {
    const user = await auth.getCurrentUser();
    if (!user) throw new Error('Not authenticated');

    const imageHash = await this.hashService.generateImageHash(imageUrl);

    const { data: existingImage } = await supabase
      .from('linkedin_profile_images')
      .select('*')
      .eq('profile_id', profileId)
      .eq('image_type', imageType)
      .eq('is_current', true)
      .maybeSingle();

    if (existingImage && existingImage.image_hash === imageHash) {
      return;
    }

    if (existingImage) {
      await supabase
        .from('linkedin_profile_images')
        .update({ is_current: false })
        .eq('id', existingImage.id);
    }

    await supabase.from('linkedin_profile_images').insert({
      profile_id: profileId,
      image_type: imageType,
      image_url: imageUrl,
      image_hash: imageHash,
      is_current: true,
      created_at: new Date().toISOString(),
    });
  }

  async runIncrementalETL(profiles: RawLinkedInProfile[]): Promise<ETLRun> {
    const runId = await this.startETLRun('incremental');

    const stats = {
      profiles_processed: 0,
      profiles_added: 0,
      profiles_updated: 0,
      profiles_unchanged: 0,
      images_processed: 0,
      validation_failures: 0,
    };

    try {
      for (const profile of profiles) {
        const result = await this.processProfile(profile, runId);
        stats.profiles_processed++;

        if (result === 'added') {
          stats.profiles_added++;
        } else if (result === 'updated') {
          stats.profiles_updated++;
        } else {
          stats.profiles_unchanged++;
        }

        const validationErrors = this.validator.validate(profile);
        if (validationErrors.some(e => e.severity === 'error')) {
          stats.validation_failures++;
        }
      }

      await this.completeETLRun(runId, stats);

      const { data } = await supabase
        .from('etl_runs')
        .select('*')
        .eq('id', runId)
        .single();

      return data as ETLRun;
    } catch (error) {
      await this.failETLRun(runId, (error as Error).message);
      throw error;
    }
  }

  async getETLRuns(limit = 10): Promise<ETLRun[]> {
    const { data, error } = await supabase
      .from('etl_runs')
      .select('*')
      .order('started_at', { ascending: false })
      .limit(limit);

    if (error) throw error;
    return data as ETLRun[];
  }

  async getProfileChangeHistory(profileId: string): Promise<ProfileChange[]> {
    const { data, error } = await supabase
      .from('profile_change_history')
      .select('*')
      .eq('profile_id', profileId)
      .order('changed_at', { ascending: false });

    if (error) throw error;
    return data as ProfileChange[];
  }
}
