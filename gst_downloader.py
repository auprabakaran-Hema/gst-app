"""
GST Portal Downloader - Automates GST return downloads from GST India Portal
Uses Selenium for browser automation
"""
import os
import time
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException


class GSTPortalDownloader:
    """Automates GST return downloads from GST India Portal"""
    
    BASE_URL = "https://www.gst.gov.in/"
    LOGIN_URL = "https://services.gst.gov.in/services/login"
    
    def __init__(self, username: str, password: str, download_dir: str = None):
        """
        Initialize GST Portal Downloader
        
        Args:
            username: GST Portal username
            password: GST Portal password
            download_dir: Directory to save downloaded files
        """
        self.username = username
        self.password = password
        self.download_dir = Path(download_dir) if download_dir else Path.home() / "Downloads" / "GST_Downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        self.driver = None
        self.wait = None
        self.logger = logging.getLogger(__name__)
        
    def _init_driver(self):
        """Initialize Chrome WebDriver with download settings"""
        chrome_options = Options()
        
        # Download preferences
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Headless mode for server deployment (optional)
        if os.environ.get('GST_HEADLESS', 'false').lower() == 'true':
            chrome_options.add_argument('--headless')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Initialize driver
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.implicitly_wait(10)
            self.wait = WebDriverWait(self.driver, 30)
            self.logger.info("Chrome WebDriver initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise
    
    def login(self) -> bool:
        """
        Login to GST Portal
        
        Returns:
            True if login successful, False otherwise
        """
        if not self.driver:
            self._init_driver()
        
        try:
            self.logger.info("Navigating to GST Portal login page...")
            self.driver.get(self.LOGIN_URL)
            time.sleep(3)
            
            # Enter username
            username_field = self.wait.until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            username_field.clear()
            username_field.send_keys(self.username)
            self.logger.info("Username entered")
            
            # Enter password
            password_field = self.driver.find_element(By.ID, "user_pass")
            password_field.clear()
            password_field.send_keys(self.password)
            self.logger.info("Password entered")
            
            # Handle CAPTCHA (manual input required or use service)
            captcha_input = self.driver.find_element(By.ID, "captcha")
            self.logger.info("Please enter CAPTCHA manually in the browser...")
            
            # Wait for login button to be clickable and click
            login_btn = self.wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
            )
            
            # Wait for CAPTCHA to be filled (manual intervention needed)
            self.logger.info("Waiting for CAPTCHA solution...")
            time.sleep(15)  # Give time for manual CAPTCHA entry
            
            # Click login
            login_btn.click()
            time.sleep(5)
            
            # Check if login successful
            if "dashboard" in self.driver.current_url.lower() or "home" in self.driver.current_url.lower():
                self.logger.info("Login successful!")
                return True
            else:
                # Check for error messages
                try:
                    error_msg = self.driver.find_element(By.CLASS_NAME, "alert-danger").text
                    self.logger.error(f"Login failed: {error_msg}")
                except:
                    self.logger.error("Login failed - unknown error")
                return False
                
        except Exception as e:
            self.logger.error(f"Error during login: {e}")
            return False
    
    def download_gstr1(self, gstin: str, period: str, fp: str) -> Optional[Path]:
        """
        Download GSTR-1 for a specific period
        
        Args:
            gstin: GSTIN number
            period: Period in format MMMYYYY (e.g., 'APR2025')
            fp: Financial period in format MMYYYY (e.g., '042025')
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading GSTR-1 for {gstin}, period: {period}")
            
            # Navigate to GSTR-1
            self.driver.get(f"https://return.gst.gov.in/returns/auth/gstr1/{gstin}")
            time.sleep(3)
            
            # Select period
            period_select = self.wait.until(
                EC.presence_of_element_located((By.ID, "finPeriod"))
            )
            period_select.send_keys(period)
            time.sleep(2)
            
            # Search/Submit
            search_btn = self.driver.find_element(By.ID, "searchBtn")
            search_btn.click()
            time.sleep(5)
            
            # Look for download button
            try:
                download_btn = self.driver.find_element(By.ID, "downloadBtn")
                download_btn.click()
                time.sleep(5)
                
                # Wait for download to complete
                downloaded_file = self._wait_for_download(".zip")
                if downloaded_file:
                    self.logger.info(f"GSTR-1 downloaded: {downloaded_file}")
                    return downloaded_file
                    
            except NoSuchElementException:
                self.logger.warning("Download button not found - may be no data for this period")
                return None
                
        except Exception as e:
            self.logger.error(f"Error downloading GSTR-1: {e}")
            return None
    
    def download_gstr2b(self, gstin: str, period: str) -> Optional[Path]:
        """
        Download GSTR-2B for a specific period
        
        Args:
            gstin: GSTIN number
            period: Period in format MMMYYYY
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading GSTR-2B for {gstin}, period: {period}")
            
            # Navigate to GSTR-2B
            self.driver.get(f"https://return.gst.gov.in/returns/auth/gstr2b/{gstin}")
            time.sleep(3)
            
            # Select period
            period_select = self.wait.until(
                EC.presence_of_element_located((By.ID, "finPeriod"))
            )
            period_select.send_keys(period)
            time.sleep(2)
            
            # Search
            search_btn = self.driver.find_element(By.ID, "searchBtn")
            search_btn.click()
            time.sleep(5)
            
            # Download
            try:
                download_btn = self.driver.find_element(By.ID, "downloadBtn")
                download_btn.click()
                time.sleep(5)
                
                downloaded_file = self._wait_for_download(".xlsx")
                if downloaded_file:
                    self.logger.info(f"GSTR-2B downloaded: {downloaded_file}")
                    return downloaded_file
                    
            except NoSuchElementException:
                self.logger.warning("Download button not found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error downloading GSTR-2B: {e}")
            return None
    
    def download_gstr2a(self, gstin: str, period: str) -> Optional[Path]:
        """
        Download GSTR-2A for a specific period
        
        Args:
            gstin: GSTIN number
            period: Period in format MMMYYYY
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading GSTR-2A for {gstin}, period: {period}")
            
            # Navigate to GSTR-2A
            self.driver.get(f"https://return.gst.gov.in/returns/auth/gstr2a/{gstin}")
            time.sleep(3)
            
            # Select period
            period_select = self.wait.until(
                EC.presence_of_element_located((By.ID, "finPeriod"))
            )
            period_select.send_keys(period)
            time.sleep(2)
            
            # Search
            search_btn = self.driver.find_element(By.ID, "searchBtn")
            search_btn.click()
            time.sleep(5)
            
            # Download
            try:
                download_btn = self.driver.find_element(By.ID, "downloadBtn")
                download_btn.click()
                time.sleep(5)
                
                downloaded_file = self._wait_for_download(".xlsx")
                if downloaded_file:
                    self.logger.info(f"GSTR-2A downloaded: {downloaded_file}")
                    return downloaded_file
                    
            except NoSuchElementException:
                self.logger.warning("Download button not found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error downloading GSTR-2A: {e}")
            return None
    
    def download_gstr3b(self, gstin: str, period: str) -> Optional[Path]:
        """
        Download GSTR-3B for a specific period
        
        Args:
            gstin: GSTIN number
            period: Period in format MMMYYYY
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            self.logger.info(f"Downloading GSTR-3B for {gstin}, period: {period}")
            
            # Navigate to GSTR-3B
            self.driver.get(f"https://return.gst.gov.in/returns/auth/gstr3b/{gstin}")
            time.sleep(3)
            
            # Select period
            period_select = self.wait.until(
                EC.presence_of_element_located((By.ID, "finPeriod"))
            )
            period_select.send_keys(period)
            time.sleep(2)
            
            # Search
            search_btn = self.driver.find_element(By.ID, "searchBtn")
            search_btn.click()
            time.sleep(5)
            
            # Download
            try:
                download_btn = self.driver.find_element(By.ID, "downloadBtn")
                download_btn.click()
                time.sleep(5)
                
                downloaded_file = self._wait_for_download(".pdf")
                if downloaded_file:
                    self.logger.info(f"GSTR-3B downloaded: {downloaded_file}")
                    return downloaded_file
                    
            except NoSuchElementException:
                self.logger.warning("Download button not found")
                return None
                
        except Exception as e:
            self.logger.error(f"Error downloading GSTR-3B: {e}")
            return None
    
    def download_all_returns(self, gstin: str, fy: str = "2025-26") -> Dict[str, List[str]]:
        """
        Download all GST returns for a financial year
        
        Args:
            gstin: GSTIN number
            fy: Financial year (e.g., '2025-26')
            
        Returns:
            Dictionary with return types as keys and lists of downloaded files as values
        """
        results = {
            "GSTR1": [],
            "GSTR2B": [],
            "GSTR2A": [],
            "GSTR3B": [],
            "errors": []
        }
        
        # Generate periods for the financial year
        periods = self._get_periods_for_fy(fy)
        
        # Login first
        if not self.login():
            results["errors"].append("Login failed")
            return results
        
        # Download each return type for each period
        for period_name, period_code, fp in periods:
            self.logger.info(f"Processing {period_name} {period_code}...")
            
            # GSTR-1
            try:
                file = self.download_gstr1(gstin, period_code, fp)
                if file:
                    results["GSTR1"].append(str(file))
            except Exception as e:
                results["errors"].append(f"GSTR-1 {period_name}: {str(e)}")
            
            # GSTR-2B
            try:
                file = self.download_gstr2b(gstin, period_code)
                if file:
                    results["GSTR2B"].append(str(file))
            except Exception as e:
                results["errors"].append(f"GSTR-2B {period_name}: {str(e)}")
            
            # GSTR-2A
            try:
                file = self.download_gstr2a(gstin, period_code)
                if file:
                    results["GSTR2A"].append(str(file))
            except Exception as e:
                results["errors"].append(f"GSTR-2A {period_name}: {str(e)}")
            
            # GSTR-3B
            try:
                file = self.download_gstr3b(gstin, period_code)
                if file:
                    results["GSTR3B"].append(str(file))
            except Exception as e:
                results["errors"].append(f"GSTR-3B {period_name}: {str(e)}")
        
        return results
    
    def _get_periods_for_fy(self, fy: str) -> List[tuple]:
        """
        Generate periods for a financial year
        
        Returns:
            List of tuples (month_name, period_code, fp)
        """
        periods = []
        start_year = int(fy.split("-")[0])
        
        months = [
            ("April", "APR"), ("May", "MAY"), ("June", "JUN"),
            ("July", "JUL"), ("August", "AUG"), ("September", "SEP"),
            ("October", "OCT"), ("November", "NOV"), ("December", "DEC"),
            ("January", "JAN"), ("February", "FEB"), ("March", "MAR")
        ]
        
        for i, (month_name, month_code) in enumerate(months):
            if i < 9:  # April-December
                year = start_year
                fp_month = f"0{i+4}"
            else:  # January-March
                year = start_year + 1
                fp_month = f"0{i-8}"
            
            period_code = f"{month_code}{year}"
            fp = f"{fp_month}{year}"
            periods.append((month_name, period_code, fp))
        
        return periods
    
    def _wait_for_download(self, extension: str, timeout: int = 60) -> Optional[Path]:
        """
        Wait for a file to be downloaded
        
        Args:
            extension: File extension to wait for
            timeout: Maximum wait time in seconds
            
        Returns:
            Path to downloaded file or None if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check for downloaded files
            files = list(self.download_dir.glob(f"*{extension}"))
            
            # Filter out temporary files
            files = [f for f in files if not f.name.endswith('.crdownload')]
            
            if files:
                # Return the most recently modified file
                return max(files, key=lambda p: p.stat().st_mtime)
            
            time.sleep(1)
        
        return None
    
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.logger.info("WebDriver closed")


def download_gst_returns(gstin: str, username: str, password: str, 
                         fy: str = "2025-26", download_dir: str = None) -> Dict:
    """
    Convenience function to download all GST returns
    
    Args:
        gstin: GSTIN number
        username: GST Portal username
        password: GST Portal password
        fy: Financial year
        download_dir: Directory to save files
        
    Returns:
        Dictionary with download results
    """
    downloader = GSTPortalDownloader(username, password, download_dir)
    
    try:
        results = downloader.download_all_returns(gstin, fy)
        return results
    finally:
        downloader.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python gst_downloader.py <gstin> <username> <password> [fy]")
        sys.exit(1)
    
    gstin = sys.argv[1]
    username = sys.argv[2]
    password = sys.argv[3]
    fy = sys.argv[4] if len(sys.argv) > 4 else "2025-26"
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Download returns
    results = download_gst_returns(gstin, username, password, fy)
    
    print("\nDownload Results:")
    print(json.dumps(results, indent=2))
