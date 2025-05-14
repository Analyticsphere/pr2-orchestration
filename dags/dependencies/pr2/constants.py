import os
from enum import Enum

# The endpoint for the pr2-transformation Cloud Run.
PR2_TRANSFORMATION_CLOUD_RUN_URL = os.environ.get("PR2_TRANSFORMATION_CLOUD_RUN_URL")

# In production, the project should be set via an environment variable.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# Datasets for the different stages of your ETL pipeline.
SOURCE_DATASET = "FlatConnect"
STAGING_DATASET = "ForTestingOnly"
CLEAN_DATASET = "CleanConnect"

# -----------------------------------------------------------------------------
# Custom Base Enum
# -----------------------------------------------------------------------------
# Define a custom base class that inherits from both str and Enum.
# This ensures that each enum member behaves like a string and can be
# printed or interpolated directly without using .value.
class StrEnum(str, Enum):
    def __str__(self):
        return f"{self.value}"

# -----------------------------------------------------------------------------
# Enums for Table Names
# -----------------------------------------------------------------------------
# We create separate enums for source, staging, and clean tables.
# Each enum sets its dataset once as a class variable (_DATASET) so that
# changes to the dataset require an update in only one place.

class SourceTables(StrEnum):
    """Enum for source tables. Uses the SOURCE_DATASET defined globally."""
    _DATASET = SOURCE_DATASET
    MODULE1_V1 = f"{PROJECT_ID}.{_DATASET}.module1_v1_JP"
    MODULE1_V2 = f"{PROJECT_ID}.{_DATASET}.module1_v2_JP"
    MODULE2_V1 = f"{PROJECT_ID}.{_DATASET}.module2_v1_JP"
    MODULE2_V2 = f"{PROJECT_ID}.{_DATASET}.module2_v2_JP"
    MODULE3 = f"{PROJECT_ID}.{_DATASET}.module3_v1_JP"
    MODULE4 = f"{PROJECT_ID}.{_DATASET}.module4_v1_JP"
    BIOSURVEY = f"{PROJECT_ID}.{_DATASET}.bioSurvey_v1_JP"
    CLINICALBIOSURVEY = f"{PROJECT_ID}.{_DATASET}.clinicalBioSurvey_v1_JP"
    COVID19SURVEY = f"{PROJECT_ID}.{_DATASET}.covid19Survey_v1_JP"
    MENSTRUALSURVEY = f"{PROJECT_ID}.{_DATASET}.menstrualSurvey_v1_JP"
    EXPERIENCE2024 = f"{PROJECT_ID}.{_DATASET}.experience2024_JP"
    MOUTHWASH = f"{PROJECT_ID}.{_DATASET}.mouthwash_v1_JP"
    BIOSPECIMEN = f"{PROJECT_ID}.{_DATASET}.biospecimen_JP"
    PARTICIPANTS = f"{PROJECT_ID}.{_DATASET}.participants_JP"

class StagingTables(StrEnum):
    """Enum for staging tables. Uses the STAGING_DATASET defined globally."""
    _DATASET = STAGING_DATASET
    # Cleaned Columns
    MODULE1_V1_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module1_v1_with_cleaned_columns"
    MODULE1_V2_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module1_v2_with_cleaned_columns"
    MODULE2_V1_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module2_v1_with_cleaned_columns"
    MODULE2_V2_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module2_v2_with_cleaned_columns"
    MODULE3_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module3_with_cleaned_columns"
    MODULE4_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.module4_with_cleaned_columns"
    BIOSURVEY_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.biosurvey_with_cleaned_columns"
    CLINICALBIOSURVEY_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.clinicalbiosurvey_with_cleaned_columns"
    COVID19SURVEY_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.covid19survey_with_cleaned_columns"
    MOUTHWASH_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.mouthwash_with_cleaned_columns"
    BIOSPECIMEN_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.biospecimen_with_cleaned_columns"
    PARTICIPANTS_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.participants_with_cleaned_columns"
    EXPERIENCE2024_CLEANED_COLUMNS = f"{PROJECT_ID}.{_DATASET}.experience2024_with_cleaned_columns"
    # Cleaned Rows
    MODULE1_V1_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module1_v1_with_cleaned_rows"
    MODULE1_V2_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module1_v2_with_cleaned_rows"
    MODULE2_V1_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module2_v1_with_cleaned_rows"
    MODULE2_V2_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module2_v2_with_cleaned_rows"
    MODULE3_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module3_with_cleaned_rows"
    MODULE4_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.module4_with_cleaned_rows"
    BIOSURVEY_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.biosurvey_with_cleaned_rows"
    CLINICALBIOSURVEY_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.clinicalbiosurvey_with_cleaned_rows"
    COVID19SURVEY_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.covid19survey_with_cleaned_rows"
    MOUTHWASH_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.mouthwash_with_cleaned_rows"
    BIOSPECIMEN_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.biospecimen_with_cleaned_rows"
    PARTICIPANTS_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.participants_with_cleaned_rows"
    EXPERIENCE2024_CLEANED_ROWS = f"{PROJECT_ID}.{_DATASET}.experience2024_with_cleaned_rows"


class CleanTables(StrEnum):
    """Enum for clean tables. Uses the CLEAN_DATASET defined globally."""
    _DATASET = CLEAN_DATASET
    MODULE1 = f"{PROJECT_ID}.{_DATASET}.module1"
    MODULE2 = f"{PROJECT_ID}.{_DATASET}.module2"
    MODULE3 = f"{PROJECT_ID}.{_DATASET}.module3"
    MODULE4 = f"{PROJECT_ID}.{_DATASET}.module4"
    BIOSURVEY = f"{PROJECT_ID}.{_DATASET}.bioSurvey"
    CLINICALBIOSURVEY = f"{PROJECT_ID}.{_DATASET}.clinicalBioSurvey"
    COVID19SURVEY = f"{PROJECT_ID}.{_DATASET}.covid19Survey"
    MENSTRUALSURVEY = f"{PROJECT_ID}.{_DATASET}.menstrualSurvey"
    EXPERIENCE2024 = f"{PROJECT_ID}.{_DATASET}.experience2024"
    MOUTHWASH = f"{PROJECT_ID}.{_DATASET}.mouthwash"
    BIOSPECIMEN = f"{PROJECT_ID}.{_DATASET}.biospecimen"
    PARTICIPANTS = f"{PROJECT_ID}.{_DATASET}.participants"

# -----------------------------------------------------------------------------
# Transformation Configuration
# -----------------------------------------------------------------------------
# The transformation configuration maps transformation group names to
# a dictionary containing a "mappings" key. Each mapping specifies the source
# table(s) and destination table(s) for a transformation.
# Using enums here helps ensure consistency and provides autocomplete.

TRANSFORM_CONFIG = {
    "clean_columns": {
        "mappings": [
            {"name": SourceTables.MODULE1_V1.name, "source": SourceTables.MODULE1_V1, "destination": StagingTables.MODULE1_V1_CLEANED_COLUMNS},
            {"name": SourceTables.MODULE1_V2.name, "source": SourceTables.MODULE1_V2, "destination": StagingTables.MODULE1_V2_CLEANED_COLUMNS},
            {"name": SourceTables.MODULE2_V1.name, "source": SourceTables.MODULE2_V1, "destination": StagingTables.MODULE2_V1_CLEANED_COLUMNS},
            {"name": SourceTables.MODULE2_V2.name, "source": SourceTables.MODULE2_V2, "destination": StagingTables.MODULE2_V2_CLEANED_COLUMNS},
            {"name": SourceTables.MODULE3.name, "source": SourceTables.MODULE3, "destination": StagingTables.MODULE3_CLEANED_COLUMNS},
            {"name": SourceTables.MODULE4.name, "source": SourceTables.MODULE4, "destination": StagingTables.MODULE4_CLEANED_COLUMNS},
            {"name": SourceTables.BIOSURVEY.name, "source": SourceTables.BIOSURVEY, "destination": StagingTables.BIOSURVEY_CLEANED_COLUMNS},
            {"name": SourceTables.CLINICALBIOSURVEY.name, "source": SourceTables.CLINICALBIOSURVEY, "destination": StagingTables.CLINICALBIOSURVEY_CLEANED_COLUMNS},
            {"name": SourceTables.COVID19SURVEY.name, "source": SourceTables.COVID19SURVEY, "destination": StagingTables.COVID19SURVEY_CLEANED_COLUMNS},
            {"name": SourceTables.MOUTHWASH.name, "source": SourceTables.MOUTHWASH, "destination": StagingTables.MOUTHWASH_CLEANED_COLUMNS},
            {"name": SourceTables.BIOSPECIMEN.name, "source": SourceTables.BIOSPECIMEN, "destination": StagingTables.BIOSPECIMEN_CLEANED_COLUMNS},
            {"name": SourceTables.PARTICIPANTS.name, "source": SourceTables.PARTICIPANTS, "destination": StagingTables.PARTICIPANTS_CLEANED_COLUMNS},
            {"name": SourceTables.EXPERIENCE2024.name, "source": SourceTables.EXPERIENCE2024, "destination": StagingTables.EXPERIENCE2024_CLEANED_COLUMNS},
            {"name": SourceTables.MENSTRUALSURVEY.name, "source": SourceTables.MENSTRUALSURVEY, "destination": CleanTables.MENSTRUALSURVEY}
        ]
    },
    "clean_rows": {
        "mappings": [
            {"name": StagingTables.MODULE1_V1_CLEANED_COLUMNS.name, "source": StagingTables.MODULE1_V1_CLEANED_COLUMNS, "destination": StagingTables.MODULE1_V1_CLEANED_ROWS},
            {"name": StagingTables.MODULE1_V2_CLEANED_COLUMNS.name, "source": StagingTables.MODULE1_V2_CLEANED_COLUMNS, "destination": StagingTables.MODULE1_V2_CLEANED_ROWS},
            {"name": StagingTables.MODULE2_V1_CLEANED_COLUMNS.name, "source": StagingTables.MODULE2_V1_CLEANED_COLUMNS, "destination": StagingTables.MODULE2_V1_CLEANED_ROWS},
            {"name": StagingTables.MODULE2_V2_CLEANED_COLUMNS.name, "source": StagingTables.MODULE2_V2_CLEANED_COLUMNS, "destination": StagingTables.MODULE2_V2_CLEANED_ROWS},
            {"name": StagingTables.MODULE3_CLEANED_COLUMNS.name, "source": StagingTables.MODULE3_CLEANED_COLUMNS, "destination": CleanTables.MODULE3},
            {"name": StagingTables.MODULE4_CLEANED_COLUMNS.name, "source": StagingTables.MODULE4_CLEANED_COLUMNS, "destination": CleanTables.MODULE4},
            {"name": StagingTables.BIOSURVEY_CLEANED_COLUMNS.name, "source": StagingTables.BIOSURVEY_CLEANED_COLUMNS, "destination": CleanTables.BIOSURVEY},
            {"name": StagingTables.CLINICALBIOSURVEY_CLEANED_COLUMNS.name, "source": StagingTables.CLINICALBIOSURVEY_CLEANED_COLUMNS, "destination": CleanTables.CLINICALBIOSURVEY},
            {"name": StagingTables.COVID19SURVEY_CLEANED_COLUMNS.name, "source": StagingTables.COVID19SURVEY_CLEANED_COLUMNS, "destination": CleanTables.COVID19SURVEY},
            {"name": StagingTables.MOUTHWASH_CLEANED_COLUMNS.name, "source": StagingTables.MOUTHWASH_CLEANED_COLUMNS, "destination": CleanTables.MOUTHWASH},
            {"name": StagingTables.BIOSPECIMEN_CLEANED_COLUMNS.name, "source": StagingTables.BIOSPECIMEN_CLEANED_COLUMNS, "destination": CleanTables.BIOSPECIMEN},
            {"name": StagingTables.PARTICIPANTS_CLEANED_COLUMNS.name, "source": StagingTables.PARTICIPANTS_CLEANED_COLUMNS, "destination": CleanTables.PARTICIPANTS},
            {"name": StagingTables.EXPERIENCE2024_CLEANED_COLUMNS.name, "source": StagingTables.EXPERIENCE2024_CLEANED_COLUMNS, "destination": CleanTables.EXPERIENCE2024}
        ]
    },
    "merge_table_versions": {
        "mappings": [
            {
                "source": [StagingTables.MODULE1_V1_CLEANED_ROWS, StagingTables.MODULE1_V2_CLEANED_ROWS], 
                "destination": CleanTables.MODULE1
            },
            {
                "source": [StagingTables.MODULE2_V1_CLEANED_ROWS, StagingTables.MODULE2_V2_CLEANED_ROWS],
                "destination": CleanTables.MODULE2
            }
        ]
    }
}
