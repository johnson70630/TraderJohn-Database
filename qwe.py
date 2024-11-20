import nltk

def setup_nltk_resources():
    """
    Downloads required NLTK resources for text processing.
    Includes the averaged perceptron tagger and other commonly needed resources.
    """
    # Download the averaged perceptron tagger
    nltk.download('averaged_perceptron_tagger')
    
    # Common additional resources that are often needed
    nltk.download('punkt')  # For sentence tokenization
    nltk.download('wordnet')  # For lemmatization
    nltk.download('stopwords')  # For stopword removal
    
    print("NLTK resources successfully downloaded!")

# Run the setup
if __name__ == "__main__":
    setup_nltk_resources()