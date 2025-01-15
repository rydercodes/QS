import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from time import sleep
from src.scrapers.base_scraper import BaseScraper

class MITScraper(BaseScraper):
    def scrape_departments(self) -> List[Dict[str, Any]]:
        try:
            departments_url = self.get_url('departments')
            self.logger.info(f"Scraping departments from: {departments_url}")
            
            response = requests.get(departments_url)
            response.raise_for_status()
            
            # Parse and process departments
            departments_data = self._process_departments(response.text)
            
            # Save data
            self._save_data(departments_data, 'departments')
            
            return departments_data
            
        except Exception as e:
            self.logger.error(f"Error scraping departments: {str(e)}")
            raise

    def _process_departments(self, html_content: str) -> Dict[str, Any]:
        """Process the HTML content and extract departments data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        selectors = self.config['selectors']['departments']
        departments_data = []
        
        sections = soup.find_all(
            selectors['container'].split('.')[0],
            class_=selectors['container'].split('.')[1]
        )
        
        for section in sections:
            department_data = self._parse_department(section, selectors)
            departments_data.append(department_data)
            sleep(1/self.config['rate_limit'])  # Respect rate limiting
        
        return {
            'university_id': self.university_data['id'],
            'scrape_date': datetime.now().isoformat(),
            'departments': departments_data
        }

    def _parse_department(self, section, selectors) -> Dict[str, Any]:
        try:
            title_elem = section.find(
                selectors['title'].split('.')[0],
                class_=selectors['title'].split('.')[1]
            )
            
            title = title_elem.text.strip()
            department_url = title_elem.get('href', '')
            
            # Generate department ID
            dept_id = self.id_generator.generate_department_id(
                self.university_data['id'],
                title  # Using title as a fallback for department number
            )

            fields = []
            field_items = section.find_all(
                selectors['fields_container'].split('.')[0],
                class_=selectors['fields_container'].split('.')[1]
            )

            for item in field_items:
                field_number = item.find(
                    selectors['field_number'].split('.')[0],
                    class_=selectors['field_number'].split('.')[1]
                ).text.strip()
                
                field = {
                    'field_id': self.id_generator.generate_field_id(
                        self.university_data['id'],
                        dept_id,
                        field_number
                    ),
                    'name': item.find(
                        selectors['field_name'].split('.')[0],
                        class_=selectors['field_name'].split('.')[1]
                    ).text.strip(),
                    'number': field_number,
                    'url': item.find('a')['href'] if item.find('a') else None
                }
                fields.append(field)

            return {
                'department_id': dept_id,
                'name': title,
                'url': department_url,
                'fields': fields
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing department: {str(e)}")
            raise

    def scrape_field_details(self, field_url: str) -> Dict[str, Any]:
        """
        Placeholder for field details scraping
        To be implemented based on field page structure
        """
        pass