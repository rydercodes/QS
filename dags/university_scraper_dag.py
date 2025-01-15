from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
        with open('config/universities.json', 'r') as f:
            universities = json.load(f)
            # For now, we're only processing MIT (first university)
            return universities[0]
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

with DAG(
    'university_departments_scraper',
    default_args=default_args,
    description='Scrape university departments data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    scrape_departments = PythonOperator(
        task_id='scrape_departments',
        python_callable=scrape_university_departments,
    )

    # Add more tasks here as needed
    scrape_departments