from datetime import datetime

class IDGenerator:
    @staticmethod
    def generate_university_file_id(university_id: int, file_type: str) -> str:
        timestamp = datetime.now().strftime('%Y%m%d')
        return f"u{str(university_id).zfill(3)}_{file_type}_{timestamp}"
    
    @staticmethod
    def generate_department_id(university_id: int, dept_number: str) -> str:
        # Clean department number of non-numeric characters
        dept_num = ''.join(filter(str.isdigit, str(dept_number))) or '000'
        return f"u{str(university_id).zfill(3)}_d{dept_num.zfill(3)}"
    
    @staticmethod
    def generate_field_id(university_id: int, dept_id: str, field_number: str) -> str:
        field_num = ''.join(filter(str.isdigit, str(field_number))) or '000'
        return f"{dept_id}_f{field_num.zfill(3)}"