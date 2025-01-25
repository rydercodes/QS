from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
from pathlib import Path
from typing import List, Dict
from src.scrapers.university_scrapers.mit_scraper import MITScraper
from src.scrapers.university_scrapers.imperial_scraper import ImperialScraper
from src.utils.logger import setup_logger

logger = setup_logger("UniversityScraperDAG")

def check_universities_for_updates() -> List[Dict]:
    """
    Check which universities need updates based on:
    1. Last update time (if exists)
    2. If no last_update time exists, include for update
    """
    try:
        config_path = Path('/opt/airflow/config/universities.json')
        logger.info(f"Loading university data from: {config_path}")
        
        with open(config_path, 'r') as f:
            universities = json.load(f)
        
        logger.info(f"Loaded universities data: {universities}")
        
        universities_to_update = []
        for university in universities:
            # Verify required fields
            required_fields = ['id', 'name', 'base_url']
            missing_fields = [field for field in required_fields if field not in university]
            if missing_fields:
                logger.error(f"University {university.get('name', 'Unknown')} missing required fields: {missing_fields}")
                continue

            logger.info(f"Processing university: {university['name']}")
            
            if 'last_updated' not in university:
                logger.info(f"No last_updated time for {university['name']}, marking for update")
                universities_to_update.append(university)
                continue
            
            last_updated = datetime.fromisoformat(university['last_updated'])
            if datetime.now() - last_updated > timedelta(hours=24):
                logger.info(f"{university['name']} needs update - last updated: {last_updated}")
                universities_to_update.append(university)
        
        logger.info(f"Universities to update: {[u['name'] for u in universities_to_update]}")
        return universities_to_update
    except Exception as e:
        logger.error(f"Error checking universities for updates: {str(e)}")
        logger.exception("Full traceback:")
        raise

def scrape_university_departments(university_data: Dict) -> None:
    """Scrape departments for a specific university"""
    try:
        logger.info(f"Received university data: {university_data}")

        # Check required fields
        required_fields = ['id', 'name', 'base_url']
        missing_fields = [field for field in required_fields if field not in university_data]
        if missing_fields:
            raise ValueError(f"Missing required fields in university data: {missing_fields}")

        logger.info(f"Starting scrape for university: {university_data['name']}")
        
        scraper_mapping = {
            "Massachusetts Institute of Technology (MIT)": MITScraper,
            "Imperial College London": ImperialScraper
        }
        
        scraper_class = scraper_mapping.get(university_data['name'])
        if not scraper_class:
            error_msg = f"No scraper found for {university_data['name']}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        scraper = scraper_class(university_data)
        departments_data = scraper.scrape_departments()
        
        # Update the last_updated timestamp
        update_university_timestamp(university_data['id'])
        
        logger.info(f"Successfully completed scrape for {university_data['name']}")
        return departments_data
    except Exception as e:
        logger.error(f"Error scraping university {university_data.get('name', 'Unknown')}: {str(e)}")
        logger.exception("Full traceback:")
        raise

def update_university_timestamp(university_id: int) -> None:
    """Update the last_updated timestamp for a university"""
    try:
        config_path = Path('/opt/airflow/config/universities.json')
        logger.info(f"Updating timestamp for university ID: {university_id}")
        
        with open(config_path, 'r') as f:
            universities = json.load(f)
        
        timestamp_updated = False
        for university in universities:
            if university['id'] == university_id:
                university['last_updated'] = datetime.now().isoformat()
                timestamp_updated = True
                logger.info(f"Updated timestamp for {university['name']}")
                break
                
        if not timestamp_updated:
            error_msg = f"University with ID {university_id} not found in configuration"
            logger.error(error_msg)
            raise ValueError(error_msg)
                
        with open(config_path, 'w') as f:
            json.dump(universities, f, indent=4)
            
        logger.info("Successfully saved updated configuration")
    except Exception as e:
        logger.error(f"Error updating university timestamp: {str(e)}")
        logger.exception("Full traceback:")
        raise

def process_university(university: Dict) -> Dict:
    """Process a single university with proper error handling"""
    try:
        logger.info(f"Processing university for scraping: {university}")
        result = scrape_university_departments(university)
        logger.info(f"Successfully scraped university: {university.get('name')}")
        return result
    except Exception as e:
        logger.error(f"Failed to scrape university {university.get('name', 'Unknown')}: {str(e)}")
        logger.exception("Full traceback:")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'university_departments_scraper',
    default_args=default_args,
    description='Scrape university departments data',
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Check which universities need updates
check_updates_task = PythonOperator(
    task_id='check_universities_for_updates',
    python_callable=check_universities_for_updates,
    dag=dag
)

# Task 2: Scrape departments for universities that need updates
scrape_departments_task = PythonOperator(
    task_id='scrape_departments',
    python_callable=lambda **context: [
        process_university(university)
        for university in context['task_instance'].xcom_pull(task_ids='check_universities_for_updates') or []
    ],
    provide_context=True,
    dag=dag
)

# Set task dependencies
check_updates_task >> scrape_departments_task

if __name__ == "__main__":
    dag.cli()