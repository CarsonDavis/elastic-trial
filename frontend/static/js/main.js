// Global variables
let currentPage = 1;
let totalPages = 0;

// Initialize on document ready
$(document).ready(function () {
    // Initialize Materialize components
    M.AutoInit();

    // Set up event handlers
    setupEventHandlers();
});

function setupEventHandlers() {
    // Search on enter key
    $('#search-input').keypress(function (e) {
        if (e.which === 13) { // Enter key
            performSearch(1);
        }
    });

    // Filter checkboxes change
    $('.doc-type-filter').change(function () {
        performSearch(1);
    });
}

function performSearch(page) {
    // Get search query
    const query = $('#search-input').val().trim();
    if (!query) {
        // Clear results if empty query
        $('#results-container').html('<p>Enter a search query to begin</p>');
        $('#pagination').empty();
        return;
    }

    // Get selected document types
    const docTypes = [];
    $('.doc-type-filter:checked').each(function () {
        docTypes.push($(this).val());
    });

    // Show loading indicator
    $('#results-container').html('<div class="progress"><div class="indeterminate"></div></div>');

    // Send search request to backend
    $.ajax({
        url: '/search',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({
            query: query,
            doc_types: docTypes,
            page: page
        }),
        success: function (response) {
            displayResults(response);
            updatePagination(response);
            currentPage = page;
        },
        error: function (error) {
            $('#results-container').html('<p>Error performing search. Please try again.</p>');
            console.error('Search error:', error);
        }
    });
}

function displayResults(response) {
    const results = response.results;
    const total = response.total;

    if (results.length === 0) {
        $('#results-container').html('<p>No results found</p>');
        return;
    }

    let html = `<p>Found ${total} results</p><ul class="collection">`;

    results.forEach(result => {
        html += `
            <li class="collection-item">
                <div>
                    <a href="${result.url}" target="_blank" class="title">${result.title}</a>
                    <p class="snippet">${result.snippet}</p>
                    <span class="badge">${result.doc_type}</span>
                </div>
            </li>
        `;
    });

    html += '</ul>';
    $('#results-container').html(html);
}

function updatePagination(response) {
    const currentPage = response.page;
    totalPages = response.pages;

    if (totalPages <= 1) {
        $('#pagination').empty();
        return;
    }

    let html = '<ul class="pagination">';

    // Previous button
    if (currentPage > 1) {
        html += `<li class="waves-effect"><a href="#!" onclick="performSearch(${currentPage - 1})"><i class="material-icons">chevron_left</i></a></li>`;
    } else {
        html += '<li class="disabled"><a href="#!"><i class="material-icons">chevron_left</i></a></li>';
    }

    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, startPage + 4);

    for (let i = startPage; i <= endPage; i++) {
        if (i === currentPage) {
            html += `<li class="active"><a href="#!">${i}</a></li>`;
        } else {
            html += `<li class="waves-effect"><a href="#!" onclick="performSearch(${i})">${i}</a></li>`;
        }
    }

    // Next button
    if (currentPage < totalPages) {
        html += `<li class="waves-effect"><a href="#!" onclick="performSearch(${currentPage + 1})"><i class="material-icons">chevron_right</i></a></li>`;
    } else {
        html += '<li class="disabled"><a href="#!"><i class="material-icons">chevron_right</i></a></li>';
    }

    html += '</ul>';
    $('#pagination').html(html);
}