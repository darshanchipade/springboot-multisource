import React, { useState } from 'react';
import './App.css';
import SearchContainer from './components/SearchContainer';
import FacetFilters from './components/FacetFilters';
import SearchResults from './components/SearchResults';

function App() {
  const [query, setQuery] = useState('');
  const [facetResponse, setFacetResponse] = useState(null);
  const [searchResults, setSearchResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const handleSearch = async (searchQuery) => {
    if (!searchQuery.trim()) return;

    setQuery(searchQuery); // Store the original query
    setIsLoading(true);
    setFacetResponse(null);
    setSearchResults([]);

    try {
      const response = await fetch(`/api/facets?query=${encodeURIComponent(searchQuery)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setFacetResponse(data);

    } catch (error) {
      console.error("Failed to fetch facets:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleFilterSelect = async (filterType, filterValue) => {
    setIsLoading(true);
    setSearchResults([]);

    const searchRequest = {
      query: query,
      tags: [],
      keywords: [],
      context: {}
    };

    if (filterType === 'tags') {
      searchRequest.tags.push(filterValue);
    } else if (filterType === 'keywords') {
      searchRequest.keywords.push(filterValue);
    } else {
      searchRequest.context[filterType] = [filterValue];
    }

    try {
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(searchRequest)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        setSearchResults(data);
        setFacetResponse(null); // Hide facets after search is complete

    } catch (error) {
        console.error("Failed to perform search:", error);
    } finally {
        setIsLoading(false);
    }
  };

  return (
    <div className="App">
      <SearchContainer onSearch={handleSearch} />
      {isLoading && <p>Loading...</p>}
      {facetResponse && !searchResults.length && (
          <FacetFilters facetData={facetResponse} onFilterSelect={handleFilterSelect} />
      )}
      {searchResults.length > 0 && <SearchResults results={searchResults} />}
    </div>
  );
}

export default App;
