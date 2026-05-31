# =============================================================================
# env_config.py
# Environment configuration management for financial platform pipelines
# =============================================================================

class Config:
    """Configuration object for a specific environment."""
    
    def __init__(self, env="dev"):
        self.env = env.lower()
        
        # Catalog and schema configurationsssss
        if self.env == "prod":
            self.catalog = "fin_platform_prod"
            self.log_level = "INFO"
        elif self.env == "uat":
            self.catalog = "fin_platform_uat"
            self.log_level = "INFO"
        else:  # dev
            self.catalog = "fin_platform_dev"
            self.log_level = "DEBUG"
        
        # Data quality settings
        self.enable_data_quality_halt = True if self.env == "prod" else False
        
        # Storage paths
        self._landing_base = f"/Volumes/{self.catalog}/landing/raw_ingest"
        self._checkpoint_base = f"/Volumes/{self.catalog}/landing/checkpoints"
    
    def bronze_table(self, table_name):
        """Get fully qualified bronze table name."""
        return f"{self.catalog}.bronze.{table_name}"
    
    def silver_table(self, table_name):
        """Get fully qualified silver table name."""
        return f"{self.catalog}.silver.{table_name}"
    
    def gold_table(self, table_name):
        """Get fully qualified gold table name."""
        return f"{self.catalog}.gold.{table_name}"
    
    def landing_path(self, dataset_name):
        """Get landing path for a dataset."""
        return f"{self._landing_base}/{dataset_name}/"
    
    def checkpoint_path(self, pipeline_name):
        """Get checkpoint path for a pipeline."""
        return f"{self._checkpoint_base}/{pipeline_name}"
    
    def audit_table(self, table_name="pipeline_runs"):
        """Get fully qualified audit table name."""
        return f"{self.catalog}.audit.{table_name}"


def get_config(env="dev"):
    """Factory function to create a Config object for the specified environment.
    
    Args:
        env: Environment name (dev, uat, prod). Defaults to 'dev'.
    
    Returns:
        Config object for the specified environment.
    """
    return Config(env)
