import React, { useState } from 'react';
import './FacetFilters.css';

const FacetFilters = ({ facetData, onFilterSelect }) => {
    const [selectedFacet, setSelectedFacet] = useState(null);
    const [selectedContextCategory, setSelectedContextCategory] = useState(null);

    if (!facetData) {
        return null;
    }

    const hasTags = facetData.tags && facetData.tags.length > 0;
    const hasKeywords = facetData.keywords && facetData.keywords.length > 0;
    const hasContext = facetData.context && Object.keys(facetData.context).length > 0;

    if (!hasTags && !hasKeywords && !hasContext) {
        return <p>No refinements available for this search.</p>;
    }

    const handleFacetSelection = (facet) => {
        setSelectedFacet(facet);
        setSelectedContextCategory(null); // Reset context category when a new facet is chosen
    };

    const renderFacetValues = () => {
        if (!selectedFacet) return null;

        if (selectedFacet === 'context') {
            if (!selectedContextCategory) {
                // Render context categories
                return (
                    <div className="facet-values">
                        {Object.keys(facetData.contextFacets).map(category => (
                            <button
                                key={category}
                                className="facet-value-button"
                                onClick={() => setSelectedContextCategory(category)}
                            >
                                {category}
                            </button>
                        ))}
                    </div>
                );
            } else {
                // Render values for the selected context category
                const values = facetData.contextFacets[selectedContextCategory];
                return (
                    <div className="facet-values">
                        <button className="back-button" onClick={() => setSelectedContextCategory(null)}>← Back</button>
                        {values.map(value => (
                            <button
                                key={value}
                                className="facet-value-button"
                                onClick={() => onFilterSelect(selectedContextCategory, value)}
                            >
                                {value}
                            </button>
                        ))}
                    </div>
                );
            }
        }

        let values = [];
        if (selectedFacet === 'tags' && hasTags) {
            values = facetData.tags;
        } else if (selectedFacet === 'keywords' && hasKeywords) {
            values = facetData.keywords;
        }

        return (
            <div className="facet-values">
                {values.map(value => (
                    <button
                        key={value}
                        className="facet-value-button"
                        onClick={() => onFilterSelect(selectedFacet, value)}
                    >
                        {value}
                    </button>
                ))}
            </div>
        );
    };

    return (
        <div className="facet-container">
            <h4>Refine your search:</h4>
            <div className="facet-categories">
                {hasTags && <button onClick={() => handleFacetSelection('tags')}>Tags</button>}
                {hasKeywords && <button onClick={() => handleFacetSelection('keywords')}>Keywords</button>}
                {hasContext && <button onClick={() => handleFacetSelection('context')}>Context</button>}
            </div>
            {renderFacetValues()}
        </div>
    );
};

export default FacetFilters;
