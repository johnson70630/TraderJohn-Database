import nltk

def download_nltk_data():
    """Download required NLTK data"""
    required_packages = [
        'punkt',
        'averaged_perceptron_tagger',
        'stopwords'
    ]
    
    for package in required_packages:
        try:
            nltk.download(package)
            print(f"Successfully downloaded {package}")
        except Exception as e:
            print(f"Error downloading {package}: {str(e)}")

if __name__ == "__main__":
    download_nltk_data()