import React from 'react';
import ReactMarkdown from 'react-markdown';

const SearchResults = ({ results }) => {
    if (!results || results.length === 0) {
        return <p>No results found.</p>;
    }

    return (
        <div className="results-container">
            <h3>Search Results</h3>
            {results.map((result, index) => (
                <div key={index} className="result-item">
                    <div className="result-metadata">
                        <span><strong>Field Name:</strong> {result.sourceFieldName}</span>
                        <span><strong>Section:</strong> {result.sectionPath}</span>
                    </div>
                    <div className="result-text">
                        <ReactMarkdown>{result.cleansedText}</ReactMarkdown>
                    </div>
                </div>
            ))}
        </div>
    );
};

export default SearchResults;
