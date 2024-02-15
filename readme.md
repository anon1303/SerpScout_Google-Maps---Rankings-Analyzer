# Google Maps Data Extractor
    This Python program extracts data from Google Maps and Google search results, providing information about local businesses and organic search rankings. It utilizes the SerpApi library to interact with the Google search engine.

## Features
- Map Pack Scraper: Scrapes data from the map pack section of Google search results, including business name, address, phone number, website, rating, and more.
- Search Ranking Analyzer: Analyzes organic search rankings, extracting information such as business domain, title, URL, and ranking position.
- Exception List Handling: Excludes certain domains from analysis based on an exception list defined in a text file.
- Customizable Query Inputs: Accepts user input for cities, states, and search queries, allowing for flexible analysis.
- Data Export: Exports extracted data to CSV files for further analysis and reporting.

## Installation
1. Clone this repository to your local machine:
    ```sh
    git clone https://github.com/yourusername/google-maps-data-extractor.git
    ```

2. Install the required dependencies using pip:

    ```sh
    pip install -r requirements.txt
    ```
## Usage
1. Ensure you have created a text file named ExceptionList.txt containing a list of domains to exclude from analysis.

2. Run the program using Python:
    ```sh
    python main.py
    ```

3. Follow the prompts to input the desired number of search results, cities, states, and search queries.

4. The program will extract data from Google Maps and Google search results, saving the results to CSV files.

## Configuration
* Exception List: Edit the ExceptionList.txt file to add or remove domains from the exclusion list.
* API Key: Replace the placeholder API key in the GMapExtractor class with your own **SerpApi** API key for access to Google search results.


## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements
- SerpApi for providing a Python library to interact with Google search results.
