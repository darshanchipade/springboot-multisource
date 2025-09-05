import React, { useState } from 'react';

const SearchContainer = ({ onSearch }) => {
    const [query, setQuery] = useState('');

    const handleSearch = (e) => {
        e.preventDefault();
        onSearch(query);
    };

    return (
        <div>
            <h1>What are you looking for?</h1>
            <form onSubmit={handleSearch}>
                <input
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="e.g., Apple Financing"
                    style={{ width: '300px', padding: '10px', marginRight: '10px' }}
                />
                <button type="submit">Search</button>
            </form>
        </div>
    );
};

export default SearchContainer;
