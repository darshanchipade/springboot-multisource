import React, { useState, useEffect } from 'react';
import Head from 'next/head';
import SearchContainer from './SearchContainer';
import ChipBar from './ChipBar';
import SearchResults from './SearchResults';

export default function Home() {
  const [query, setQuery] = useState('');
  const [refinementChips, setRefinementChips] = useState([]);
  const [activeChips, setActiveChips] = useState([]);
  const [searchResults, setSearchResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isSearching, setIsSearching] = useState(false);

  useEffect(() => {
    if (query.trim() === '' || activeChips.length === 0) {
        if (searchResults.length > 0) {
            setSearchResults([]);
        }
        return;
    }

    const performSearch = async () => {
        setIsSearching(true);
        setSearchResults([]);

        const searchRequest = {
            query: query,
            tags: [],
            keywords: [],
            context: {}
        };

        activeChips.forEach(chip => {
            if (chip.type === 'Tag') {
                searchRequest.tags.push(chip.value);
            } else if (chip.type === 'Keyword') {
                searchRequest.keywords.push(chip.value);
            } else if (chip.type.startsWith('Context:')) {
                const fullPath = chip.type.substring("Context:".length);
                const pathParts = fullPath.split('.');

                let currentLevel = searchRequest.context;
                for (let i = 0; i < pathParts.length - 1; i++) {
                    const part = pathParts[i];
                    currentLevel[part] = currentLevel[part] || {};
                    currentLevel = currentLevel[part];
                }

                const lastPart = pathParts[pathParts.length - 1];
                currentLevel[lastPart] = currentLevel[lastPart] || [];
                if (!currentLevel[lastPart].includes(chip.value)) {
                    currentLevel[lastPart].push(chip.value);
                }
            }
        });

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
        } catch (error) {
            console.error("Failed to perform search:", error);
        } finally {
            setIsSearching(false);
        }
    };

    performSearch();

  }, [activeChips, query]);


  const handleInitialSearch = async (searchQuery) => {
    if (!searchQuery.trim()) return;

    setQuery(searchQuery);
    setIsLoading(true);
    setRefinementChips([]);
    setActiveChips([]);
    setSearchResults([]);

    try {
      const response = await fetch(`/api/refine?query=${encodeURIComponent(searchQuery)}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setRefinementChips(data);
    } catch (error) {
      console.error("Failed to fetch refinement chips:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleChipClick = (chipToToggle) => {
    setActiveChips(prevActiveChips => {
        const isChipActive = prevActiveChips.some(activeChip =>
            activeChip.value === chipToToggle.value && activeChip.type === chipToToggle.type
        );

        if (isChipActive) {
            return prevActiveChips.filter(activeChip =>
                !(activeChip.value === chipToToggle.value && activeChip.type === chipToToggle.type)
            );
        } else {
            return [...prevActiveChips, chipToToggle];
        }
    });
  };

  return (
    <div className="App">
        <Head>
            <title>Intelligent Search</title>
            <meta name="description" content="Next.js intelligent search interface" />
            <link rel="icon" href="/favicon.ico" />
        </Head>

        <main>
            <SearchContainer onSearch={handleInitialSearch} />
            {isLoading && <p>Loading suggestions...</p>}
            <ChipBar chips={refinementChips} onChipClick={handleChipClick} activeChips={activeChips} />
            {isSearching && <p>Searching...</p>}
            {searchResults.length > 0 && <SearchResults results={searchResults} />}
        </main>
    </div>
  );
}
