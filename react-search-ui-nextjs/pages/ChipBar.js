import React from 'react';

const ChipBar = ({ chips, onChipClick, activeChips }) => {
    if (!chips || chips.length === 0) {
        return null;
    }

    const isChipActive = (chip) => {
        return activeChips.some(activeChip =>
            activeChip.value === chip.value && activeChip.type === chip.type
        );
    };

    return (
        <div className="chip-bar">
            <strong>Refine by:</strong>
            {chips.map((chip, index) => (
                <button
                    key={`${chip.type}-${chip.value}`}
                    className={`chip ${isChipActive(chip) ? 'active' : ''}`}
                    onClick={() => onChipClick(chip)}
                >
                    {chip.type}: {chip.value} ({chip.count})
                </button>
            ))}
        </div>
    );
};

export default ChipBar;
