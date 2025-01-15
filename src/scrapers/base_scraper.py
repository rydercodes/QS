from abc import ABC, abstractmethod
import yaml
import json
import os
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Dict, Any
from src.utils.logger import setup_logger
from src.utils.id_generator import IDGenerator

class BaseScraper(ABC):
    def __init__(self, university_data: dict):
        self.university_data = university_data
        self.config = self._load_config()
        self.logger = setup_logger(self.university_data['name'])
        self.id_generator = IDGenerator()

    def _load_config(self) -> dict:
        try:
            with open('config/university_configs.yaml', 'r') as f:
                configs = yaml.safe_load(f)
                university_key = self.university_data['name'].split('(')[0].strip().replace(' ', '_')
                config = configs.get(university_key)
                if not config:
                    raise ValueError(f"No configuration found for {university_key}")
                return config
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")

    def get_url(self, url_type: str) -> str:
        if url_type not in self.config['urls']:
            raise ValueError(f"URL type {url_type} not found in configuration")
        return urljoin(self.university_data['base_url'], self.config['urls'][url_type])

    def _save_data(self, data: Dict[str, Any], data_type: str) -> str:
        """Save scraped data to file"""
        try:
            # Generate file path
            file_id = self.id_generator.generate_university_file_id(
                self.university_data['id'],
                data_type
            )
            
            date_folder = datetime.now().strftime('%Y%m%d')
            university_folder = f"u{str(self.university_data['id']).zfill(3)}"
            
            path = os.path.join(
                'data',
                'raw',
                'universities',
                university_folder,
                date_folder
            )
            
            os.makedirs(path, exist_ok=True)
            file_path = os.path.join(path, f"{data_type}.json")
            
            # Save data
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            
            self.logger.info(f"Data saved successfully to {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            raise

    @abstractmethod
    def scrape_departments(self) -> List[Dict[str, Any]]:
        """Scrape departments data"""
        pass

    @abstractmethod
    def scrape_field_details(self, field_url: str) -> Dict[str, Any]:
        """Scrape specific field details"""
        pass