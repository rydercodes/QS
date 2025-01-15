from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import json
from src.scrapers.university_scrapers.mit_scraper import MITScraper
from src.utils.logger import setup_logger

logger = setup_logger("UniversityScraperDAG")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def load_university_data():
    """Load university data from configuration"""
    try:
        config_path = Path('/opt/airflow/config/universities.json')
        logger.info(f"Loading university data from: {config_path}")
        
        with open(config_path, 'r') as f:
            universities = json.load(f)
            # For now, we're only processing MIT (first university)
            university = universities[0]
            logger.info(f"Loaded data for university: {university['name']}")
            return university
    except Exception as e:
        logger.error(f"Error loading university data: {str(e)}")
        raise

def scrape_university_departments():
    """Scrape departments for a university"""
    try:
        university_data = load_university_data()
        logger.info(f"Starting scraping for {university_data['name']}")
        
        scraper = MITScraper(university_data)
        departments_data = scraper.scrape_departments()
        
        logger.info(f"Successfully scraped {len(departments_data['departments'])} departments")
        return departments_data
    except Exception as e:
        logger.error(f"Error in scraping process: {str(e)}")
        raise

def verify_scraped_data():
    """Verify scraped data and show contents"""
    try:
        base_path = Path('/opt/airflow/data/universities')
        logger.info(f"Checking for scraped data in: {base_path}")
        
        if not base_path.exists():
            logger.warning("Data directory does not exist!")
            return
        
        for uni_dir in base_path.glob('u*'):
            logger.info(f"Checking university directory: {uni_dir}")
            for date_dir in uni_dir.glob('*'):
                if date_dir.is_dir():
                    for data_file in date_dir.glob('*.json'):
                        logger.info(f"Found data file: {data_file}")
                        with open(data_file) as f:
                            data = json.load(f)
                            dept_count = len(data.get('departments', []))
                            logger.info(f"Found {dept_count} departments in {data_file.name}")
    except Exception as e:
        logger.error(f"Error checking scraped data: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'university_departments_scraper',
    default_args=default_args,
    description='Scrape university departments data',
    schedule_interval='@daily',
    catchup=False
)

# Define tasks
scrape_task = PythonOperator(
    task_id='scrape_departments',
    python_callable=scrape_university_departments,
    dag=dag
)

verify_task = PythonOperator(
    task_id='verify_data',
    python_callable=verify_scraped_data,
    dag=dag
)

# Set task dependencies
scrape_task >> verify_task