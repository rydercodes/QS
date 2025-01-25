# src/scrapers/university_scrapers/imperial_scraper.py

from src.scrapers.base_scraper import BaseScraper
from typing import Dict, Any
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep

class ImperialScraper(BaseScraper):
    def scrape_departments(self) -> Dict[str, Any]:
        """Scrape departments data from Imperial College London"""
        try:
            base_url = self.university_data['base_url']
            self.logger.info(f"Starting Imperial College scraper with URL: {base_url}")
            
            self.logger.info("Making HTTP request...")
            response = requests.get(base_url, timeout=self.config['timeout'])
            self.logger.info(f"Response status: {response.status_code}")
            response.raise_for_status()
            
            departments_data = self._process_departments(response.text)
            self.logger.info(f"Found faculties: {[d['name'] for d in departments_data['departments']]}")
            
            saved_path = self._save_data(departments_data, 'departments')
            self.logger.info(f"Data saved to: {saved_path}")
            return departments_data
            
        except Exception as e:
            self.logger.error(f"Error scraping departments: {str(e)}")
            self.logger.exception("Full traceback:")
            raise

    def _process_departments(self, html_content: str) -> Dict[str, Any]:
        """Process the HTML content and extract departments data"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            departments_data = []
            
            # Find all faculty sections
            faculty_sections = soup.find_all('div', class_='col lg-3 md-6 link-list')
            
            for faculty_section in faculty_sections:
                # Get faculty info from div.module.equal-height
                module = faculty_section.find('div', class_='module equal-height')
                if not module:
                    continue

                faculty_data = self._parse_faculty(module)
                if faculty_data:
                    departments_data.append(faculty_data)
                    sleep(1/self.config['rate_limit'])  # Respect rate limiting
            
            return {
                'university_id': self.university_data['id'],
                'scrape_date': datetime.now().isoformat(),
                'departments': departments_data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing departments: {str(e)}")
            raise

    def _parse_faculty(self, module) -> Dict[str, Any]:
        """Parse individual faculty section"""
        try:
            faculty_title = module.find('h2', class_='fake-h3')
            if not faculty_title:
                return None
                
            faculty_link = faculty_title.find('a')
            faculty_name = faculty_link.text.strip()
            faculty_url = faculty_link['href']
            
            # Generate faculty ID
            faculty_id = self.id_generator.generate_department_id(
                self.university_data['id'],
                faculty_name
            )
            
            # Get departments from ul inside the module
            departments = []
            department_list = module.find('ul')
            if department_list:
                department_items = department_list.find_all('li')
                for idx, dept in enumerate(department_items, 1):
                    dept_link = dept.find('a')
                    if dept_link:
                        department = {
                            'field_id': self.id_generator.generate_field_id(
                                self.university_data['id'],
                                faculty_id,
                                idx
                            ),
                            'name': dept_link.text.strip(),
                            'url': dept_link['href']
                        }
                        departments.append(department)
                
            return {
                'department_id': faculty_id,
                'name': faculty_name,
                'url': faculty_url,
                'fields': departments
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing faculty section: {str(e)}")
            raise

    def scrape_field_details(self, field_url: str) -> Dict[str, Any]:
        """
        Placeholder for field details scraping
        To be implemented based on field page structure
        """
        pass