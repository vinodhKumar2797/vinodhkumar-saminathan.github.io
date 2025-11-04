/*
  # Fix RLS Policies for ETL Runs and Profile Images

  ## Changes
  - Fix INSERT policy for etl_runs to allow user_id in WITH CHECK
  - Remove user_id requirement from linkedin_profile_images (use profile_id check instead)
  - Fix profile_change_history RLS policies
*/

DROP POLICY IF EXISTS "Users can insert ETL runs" ON etl_runs;

CREATE POLICY "Users can insert ETL runs"
  ON etl_runs FOR INSERT
  TO authenticated
  WITH CHECK (user_id = auth.uid());

DROP POLICY IF EXISTS "Users can insert profile images for own profiles" ON linkedin_profile_images;

CREATE POLICY "Users can insert profile images for own profiles"
  ON linkedin_profile_images FOR INSERT
  TO authenticated
  WITH CHECK (
    EXISTS (
      SELECT 1 FROM linkedin_profiles
      WHERE linkedin_profile_images.profile_id = linkedin_profiles.id
      AND linkedin_profiles.user_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS "Users can view profile changes of own profiles" ON profile_change_history;
DROP POLICY IF EXISTS "Users can insert profile changes for own profiles" ON profile_change_history;

CREATE POLICY "Users can view profile changes of own profiles"
  ON profile_change_history FOR SELECT
  TO authenticated
  USING (
    EXISTS (
      SELECT 1 FROM linkedin_profiles
      WHERE profile_change_history.profile_id = linkedin_profiles.id
      AND linkedin_profiles.user_id = auth.uid()
    )
  );

CREATE POLICY "Users can insert profile changes for own profiles"
  ON profile_change_history FOR INSERT
  TO authenticated
  WITH CHECK (
    EXISTS (
      SELECT 1 FROM linkedin_profiles
      WHERE profile_change_history.profile_id = linkedin_profiles.id
      AND linkedin_profiles.user_id = auth.uid()
    )
  );
