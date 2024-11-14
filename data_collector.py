import logging.handlers
import pandas as pd
import logging

class DataCollector:
    
    def __init__(self, save_path: str) -> None:
        """
        Initialize the datacollector with the save path to save collected data

        :param save_path: Directory to save collected files
        """
        self.save_path = save_path
        self.collectors = {
            'normal': self._collect_normal_traffic,
            'attack': self._collect_attack_traffic,
            'synthetic': self._generate_synthetic_data
        }

        # Initialize Logger
        self._setup_logger()

    async def collect_data(self, collection_type: str, duration: int) -> None:
        """
        To collect specific type of data

        :param collection_type: Type of data to collect ('normal', 'attack', 'synthetic')
        :param duration: Duration in seconds for which to collect data 
        """
        if collection_type not in self.collectors:
            self.logger.error(f'Invalid collection type: {collection_type}')
            raise Exception("Invalid collector")
        
        self.logger.info(f"Capturing {collection_type} traffic for {duration} seconds")
        collector = self.collectors.get(collection_type)
        try:
            data = await collector(duration)
            self.logger.info(f'Data Collected for {collection_type} traffic for {duration} seconds successfully')
        except Exception as e:
            self.logger.exception(f"Error occured during data collection for type {collection_type}", exc_info=e)
        
        self._save_data(data, collection_type)

    def _save_data(self, data: pd.DataFrame, collection_type: str) -> None:
        """
        Save collected data to specified directory with an unique filename

        :param data: DataFrame of collected network traffic
        :param collection_type: Type of collected data (for file naming)
        """
        self.logger.info("Saving captured traffic")
        try:
            data.to_csv(f'{self.save_path}/{collection_type}_data.csv')
        except IsADirectoryError as d:
            self.logger.exception("Failed to save data", exc_info=d)

    def _setup_logger(self) -> None:
        """
        Configure logging settings
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        handler = logging.FileHandler('logs/collectorlog.log')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        self.logger.addHandler(handler)