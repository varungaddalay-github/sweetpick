console.log('SweetPickApp JavaScript file loaded successfully!');

class SweetPickApp {
    constructor() {
        console.log('SweetPickApp constructor called');
        this.apiBase = '';
        this.currentQuery = '';
        this.favorites = JSON.parse(localStorage.getItem('sweetpick_favorites') || '[]');
        this.chatSessionId = localStorage.getItem('sweetpick_chat_session') || '';
        this.init();
    }

    init() {
        this.bindEvents();
        this.loadFavorites();
        this.setupQuickFilters();
        this.setupNeighborhoodSelector();
        
        console.log('SweetPickApp initialized with neighborhood selector setup');
        
        // Test the shouldUseChat function
        console.log('Testing shouldUseChat function:');
        console.log('"what to order" ->', this.shouldUseChat('what to order'));
        console.log('"follow up" ->', this.shouldUseChat('follow up'));
        console.log('"more questions" ->', this.shouldUseChat('more questions'));
    }

    bindEvents() {
        // Search form submission
        const searchForm = document.getElementById('searchForm');
        const searchInput = document.getElementById('searchInput');

        searchForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const query = searchInput.value.trim();
            if (!query) return;

            if (this.shouldUseChat(query)) {
                this.toggleChat(true);
                this.sendChatMessage(query); // send directly to chat
                return;
            }

            this.performSearch(query);
        });

        // Search input real-time feedback
        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            if (query.length > 2) {
                this.showSearchSuggestions(query);
            }
        });

        // Quick filter buttons
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const query = e.target.dataset.query;
                searchInput.value = query;
                this.performSearch(query);
            });
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === '/' && !e.target.matches('input, textarea')) {
                e.preventDefault();
                searchInput.focus();
            }
        });

        const chatOpenBtn = document.getElementById('chatOpenBtn');
        const chatCloseBtn = document.getElementById('chatCloseBtn');
        const chatSendBtn = document.getElementById('chatSendBtn');
        const chatInput = document.getElementById('chatInput');

        chatOpenBtn.addEventListener('click', ()=> this.toggleChat(true));
        chatCloseBtn.addEventListener('click', ()=> this.toggleChat(false));
        chatSendBtn.addEventListener('click', ()=> this.sendChatMessage());
        chatInput.addEventListener('keydown', (e)=>{ if(e.key==='Enter') this.sendChatMessage(); });
    }

    setupQuickFilters() {
        // Add smooth hover effects to filter buttons
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('mouseenter', () => {
                btn.style.transform = 'translateY(-2px) scale(1.05)';
            });
            
            btn.addEventListener('mouseleave', () => {
                btn.style.transform = 'translateY(0) scale(1)';
            });
        });
    }

    setupNeighborhoodSelector() {
        console.log('Setting up neighborhood selector...');
        
        const toggle = document.getElementById('neighborhoodToggle');
        const dropdown = document.getElementById('neighborhoodDropdown');
        const neighborhoodList = document.getElementById('neighborhoodList');
        const selectedNeighborhood = document.getElementById('selectedNeighborhood');
        
        console.log('Neighborhood elements found:', {
            toggle: !!toggle,
            dropdown: !!dropdown,
            neighborhoodList: !!neighborhoodList,
            selectedNeighborhood: !!selectedNeighborhood
        });
        
        // Check if elements exist
        if (!toggle) console.error('❌ neighborhoodToggle element not found');
        if (!dropdown) console.error('❌ neighborhoodDropdown element not found');
        if (!neighborhoodList) console.error('❌ neighborhoodList element not found');
        if (!selectedNeighborhood) console.error('❌ selectedNeighborhood element not found');
        
        // Log the actual elements
        console.log('Actual elements:', {
            toggle: toggle,
            dropdown: dropdown,
            neighborhoodList: neighborhoodList,
            selectedNeighborhood: selectedNeighborhood
        });

        // Toggle dropdown
        toggle.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            
            console.log('Toggle clicked, current display:', dropdown.style.display);
            
            const isVisible = dropdown.style.display === 'block';
            dropdown.style.display = isVisible ? 'none' : 'block';
            
            console.log('Dropdown display set to:', dropdown.style.display);
            
            if (!isVisible) {
                this.loadNeighborhoods();
            }
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!toggle.contains(e.target) && !dropdown.contains(e.target)) {
                dropdown.style.display = 'none';
            }
        });

        // Load neighborhoods when search input changes to detect city
        document.getElementById('searchInput').addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase();
            this.detectCityAndShowNeighborhoods(query);
        });
    }

    detectCityAndShowNeighborhoods(query) {
        const neighborhoodSelector = document.getElementById('neighborhoodSelector');
        const cities = ['manhattan', 'jersey city', 'hoboken', 'new york', 'nyc', 'brooklyn', 'queens', 'bronx'];
        
        const detectedCity = cities.find(city => query.includes(city));
        
        console.log('City detection:', { 
            query, 
            detectedCity, 
            neighborhoodSelector: !!neighborhoodSelector,
            neighborhoodSelectorElement: neighborhoodSelector
        });
        
        if (detectedCity) {
            if (neighborhoodSelector) {
                neighborhoodSelector.style.display = 'block';
                this.currentCity = detectedCity;
                console.log('✅ Neighborhood selector shown for:', detectedCity);
            } else {
                console.error('❌ neighborhoodSelector element not found when trying to show it');
            }
        } else {
            if (neighborhoodSelector) {
                neighborhoodSelector.style.display = 'none';
            }
        }
    }

    async loadNeighborhoods() {
        console.log('Loading neighborhoods...');
        
        const neighborhoodList = document.getElementById('neighborhoodList');
        const city = this.currentCity || 'Manhattan';
        
        // Capitalize the city name for the API
        const capitalizedCity = city.charAt(0).toUpperCase() + city.slice(1);
        
        console.log('Loading neighborhoods for city:', capitalizedCity);
        
        try {
            const response = await fetch(`/api/neighborhoods/${capitalizedCity}`);
            const data = await response.json();
            
            console.log('API response:', data);
            console.log('Neighborhoods loaded:', data.neighborhoods ? data.neighborhoods.length : 0);
            
            neighborhoodList.innerHTML = '';
            
            if (data.neighborhoods && data.neighborhoods.length > 0) {
                data.neighborhoods.forEach(neighborhood => {
                    const item = this.createNeighborhoodItem(neighborhood);
                    neighborhoodList.appendChild(item);
                });
                
                console.log('Neighborhood items created and added to DOM');
            } else {
                console.error('No neighborhoods returned from API');
                neighborhoodList.innerHTML = '<div class="neighborhood-item">No neighborhoods found for this city</div>';
            }
        } catch (error) {
            console.error('Error loading neighborhoods:', error);
            neighborhoodList.innerHTML = '<div class="neighborhood-item">Error loading neighborhoods</div>';
        }
    }

    createNeighborhoodItem(neighborhood) {
        console.log('Creating neighborhood item for:', neighborhood.name);
        
        const item = document.createElement('div');
        item.className = 'neighborhood-item';
        item.dataset.neighborhood = neighborhood.name;
        
        // Clean styling - no debug borders
        
        const cuisineTags = neighborhood.cuisine_focus.slice(0, 3).map(cuisine => 
            `<span class="cuisine-tag">${cuisine}</span>`
        ).join('');
        
        item.innerHTML = `
            <div class="neighborhood-info">
                <div class="neighborhood-name" style="font-weight: bold; font-size: 16px;">${neighborhood.name}</div>
                <div class="neighborhood-description">${neighborhood.description}</div>
                <div class="neighborhood-cuisines">${cuisineTags}</div>
            </div>
            <div class="neighborhood-meta">
                <span>${Math.round(neighborhood.tourist_factor * 100)}% tourist</span>
                <span>•</span>
                <span>${neighborhood.price_level}</span>
            </div>
        `;
        
        item.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            console.log('🎯 Neighborhood item clicked:', neighborhood.name);
            this.selectNeighborhood(neighborhood);
        });
        
        // Clean hover effects
        
        return item;
    }

    selectNeighborhood(neighborhood) {
        console.log('Selecting neighborhood:', neighborhood.name);
        
        const selectedNeighborhood = document.getElementById('selectedNeighborhood');
        const dropdown = document.getElementById('neighborhoodDropdown');
        const neighborhoodList = document.getElementById('neighborhoodList');
        
        // Update selected text
        selectedNeighborhood.textContent = neighborhood.name;
        console.log('Updated selected neighborhood text to:', neighborhood.name);
        
        // Update visual selection
        neighborhoodList.querySelectorAll('.neighborhood-item').forEach(item => {
            item.classList.remove('selected');
        });
        
        const selectedItem = neighborhoodList.querySelector(`[data-neighborhood="${neighborhood.name}"]`);
        if (selectedItem) {
            selectedItem.classList.add('selected');
            console.log('Added selected class to item');
        }
        
        // Close dropdown
        dropdown.style.display = 'none';
        console.log('Closed dropdown');
        
        // Store selected neighborhood
        this.selectedNeighborhood = neighborhood;
        console.log('Stored selected neighborhood:', this.selectedNeighborhood);
        
        // Update search input with neighborhood context
        const searchInput = document.getElementById('searchInput');
        const currentQuery = searchInput.value;
        if (currentQuery && !currentQuery.includes(neighborhood.name)) {
            searchInput.value = `${currentQuery} in ${neighborhood.name}`;
            console.log('Updated search input to:', searchInput.value);
        }
    }



    async performSearch(query) {
        this.currentQuery = query;
        this.showLoading();
        this.showResults();

        // Add neighborhood context if selected
        let requestBody = { query: query };
        if (this.selectedNeighborhood) {
            requestBody.neighborhood = this.selectedNeighborhood.name;
            requestBody.neighborhood_context = {
                city: this.currentCity,
                cuisine_focus: this.selectedNeighborhood.cuisine_focus,
                restaurant_types: this.selectedNeighborhood.restaurant_types,
                iconic_dishes: this.selectedNeighborhood.iconic_dishes,
                tourist_factor: this.selectedNeighborhood.tourist_factor,
                price_level: this.selectedNeighborhood.price_level
            };
        }

        try {
            const response = await fetch('/query', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            this.displayResults(data);
            
        } catch (error) {
            console.error('Search error:', error);
            this.showError('Sorry, something went wrong. Please try again.');
        } finally {
            this.hideLoading();
        }
    }

    showLoading() {
        const loadingState = document.getElementById('loadingState');
        const resultsGrid = document.getElementById('resultsGrid');
        const aiResponse = document.getElementById('aiResponse');
        
        loadingState.style.display = 'block';
        resultsGrid.style.display = 'none';
        aiResponse.style.display = 'none';
        
        loadingState.classList.add('fade-in');
        
        // Add dynamic cooking messages
        this.startCookingMessages();
    }

    hideLoading() {
        const loadingState = document.getElementById('loadingState');
        loadingState.style.display = 'none';
        
        // Clear cooking messages interval
        if (this.loadingInterval) {
            clearInterval(this.loadingInterval);
            this.loadingInterval = null;
        }
    }

    showResults() {
        const resultsSection = document.getElementById('resultsSection');
        resultsSection.style.display = 'block';
        resultsSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    displayResults(data) {
        this.updateResultsHeader(data);
        this.displayAIResponse(data.natural_response || data.fallback_reason, data); // Pass data parameter
        this.displayRecommendations(data.recommendations);
        
        // Add animation
        setTimeout(() => {
            document.getElementById('resultsGrid').classList.add('fade-in');
        }, 100);
    }

    updateResultsHeader(data) {
        const resultsCount = document.getElementById('resultsCount');
        const resultsTime = document.getElementById('resultsTime');
        
        const count = data.recommendations ? data.recommendations.length : 0;
        resultsCount.textContent = `${count} result${count !== 1 ? 's' : ''}`;
        resultsTime.textContent = `${data.processing_time?.toFixed(2) || '0.00'}s`;
    }

    displayAIResponse(naturalResponse, data = null) {
        const aiResponse = document.getElementById('aiResponse');
        
        if (naturalResponse && naturalResponse.trim()) {
            // Get cuisine-specific emoji
            const cuisineEmoji = this.getCuisineResponseEmoji(data);
            
            aiResponse.innerHTML = `
                <h3>${cuisineEmoji} SweetPick Recommendation</h3>
                <p>${this.formatAIResponse(naturalResponse)}</p>
                <div class="ai-cta">
                    <button id="continueChatBtn" class="btn-secondary">
                        <span>🍽️</span> More tasty picks?
                    </button>
                </div>
            `;
            aiResponse.style.display = 'block';
            aiResponse.classList.add('fade-in');

            const btn = document.getElementById('continueChatBtn');
            if (btn) {
                btn.onclick = () => this.openChatWithContext();
            }
        } else {
            aiResponse.style.display = 'none';
        }
    }

    formatAIResponse(response) {
        // Add some nice formatting to the AI response
        return response
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/\n/g, '<br>');
    }

    displayRecommendations(recommendations) {
        const resultsGrid = document.getElementById('resultsGrid');
        
        if (!recommendations || recommendations.length === 0) {
            resultsGrid.innerHTML = `
                <div class="no-results">
                    <div class="no-results-icon">🔍</div>
                    <h3>No results found</h3>
                    <p>Try a different search or browse our quick picks above.</p>
                </div>
            `;
            return;
        }

        resultsGrid.innerHTML = recommendations.map(item => this.createResultCard(item)).join('');
        resultsGrid.style.display = 'grid';
    }

    createResultCard(item) {
        const isFavorite = this.favorites.some(fav => 
            fav.dish_name === item.dish_name && fav.restaurant_name === item.restaurant_name
        );

        const cuisineIcon = this.getCuisineIcon(item.cuisine_type);
        const rating = item.restaurant_rating || item.rating || 0;
        const score = item.recommendation_score || item.confidence || 0;

        const reason = item.reason ? `<p class="result-reason">${item.reason}</p>` : '';
        const badge = item.type === 'web_search' ? `<span class="result-fallback">Web Search</span>` : (item.fallback_reason ? `<span class="result-fallback">Alternative</span>` : '');

        return `
            <div class="result-card" data-dish="${item.dish_name}" data-restaurant="${item.restaurant_name}">
                <div class="result-image">
                    ${cuisineIcon}
                    <button class="favorite-btn ${isFavorite ? 'active' : ''}" onclick="sweetPickApp.toggleFavorite(this)">
                        <i class="fas fa-heart"></i>
                    </button>
                </div>
                <div class="result-content">
                    <div class="result-header">
                        <div>
                            <h3 class="result-title">${item.dish_name || item.restaurant_name || 'Recommendation'}</h3>
                            <p class="result-restaurant">${item.restaurant_name || item.location || ''}</p>
                        </div>
                        <div class="result-rating">
                            <i class="fas fa-star"></i>
                            ${(rating || 0).toFixed(1)}
                        </div>
                    </div>
                    <div class="result-details">
                        <span><i class="fas fa-map-marker-alt"></i> ${item.location || item.cuisine_type || ''}</span>
                        ${item.type ? `<span><i class="fas fa-tag"></i> ${item.type === 'web_search' ? 'Web Search' : item.type}</span>` : ''}
                    </div>
                    ${reason}
                    ${this.renderDishBadges(item)}
                    <div class="result-footer">
                        <span class="result-score">Score: ${((score || 0) * 100).toFixed(0)}%</span>
                        ${badge}
                    </div>
                </div>
            </div>
        `;
    }

    getCuisineIcon(cuisine) {
        const icons = {
            'Italian': '🍝',
            'Indian': '🍛',
            'Chinese': '🥢',
            'Mexican': '🌮',
            'American': '🍔',
            'Mediterranean': '🫒',
            'Japanese': '🍱',
            'Thai': '🍜',
            'French': '🥐',
            'Greek': '🥙'
        };
        return icons[cuisine] || '🍽️';
    }

    toggleFavorite(button) {
        const card = button.closest('.result-card');
        const dishName = card.dataset.dish;
        const restaurantName = card.dataset.restaurant;
        
        const favorite = {
            dish_name: dishName,
            restaurant_name: restaurantName,
            timestamp: new Date().toISOString()
        };

        const existingIndex = this.favorites.findIndex(fav => 
            fav.dish_name === dishName && fav.restaurant_name === restaurantName
        );

        if (existingIndex > -1) {
            // Remove from favorites
            this.favorites.splice(existingIndex, 1);
            button.classList.remove('active');
            this.showToast('Removed from favorites', 'info');
        } else {
            // Add to favorites
            this.favorites.push(favorite);
            button.classList.add('active');
            this.showToast('Added to favorites!', 'success');
        }

        localStorage.setItem('sweetpick_favorites', JSON.stringify(this.favorites));
    }

    loadFavorites() {
        // Could add a favorites section or indicator
        console.log(`Loaded ${this.favorites.length} favorites`);
    }

    showSearchSuggestions(query) {
        // Simple suggestion logic - could be enhanced with API call
        const suggestions = [
            'Italian food in Manhattan',
            'Indian biryani in Jersey City',
            'Best pizza near me',
            'Spicy vegetarian dishes',
            'Chinese takeout in Hoboken'
        ].filter(suggestion => 
            suggestion.toLowerCase().includes(query.toLowerCase())
        );

        // Could implement dropdown suggestions here
    }

    showError(message) {
        const resultsGrid = document.getElementById('resultsGrid');
        resultsGrid.innerHTML = `
            <div class="error-state">
                <div class="error-icon">⚠️</div>
                <h3>Oops! Something went wrong</h3>
                <p>${message}</p>
                <button class="retry-btn" onclick="sweetPickApp.performSearch('${this.currentQuery}')">
                    Try Again
                </button>
            </div>
        `;
        resultsGrid.style.display = 'block';
    }

    showToast(message, type = 'info') {
        // Create toast notification
        const toast = document.createElement('div');
        toast.className = `toast toast-${type}`;
        toast.innerHTML = `
            <i class="fas fa-${type === 'success' ? 'check' : 'info'}"></i>
            <span>${message}</span>
        `;
        
        document.body.appendChild(toast);
        
        // Show toast
        setTimeout(() => toast.classList.add('show'), 100);
        
        // Hide toast
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => document.body.removeChild(toast), 300);
        }, 3000);
    }

    getCuisineResponseEmoji(data) {
        // Try to detect cuisine from recommendations or query
        let detectedCuisine = null;
        
        if (data && data.recommendations && data.recommendations.length > 0) {
            // Get the most common cuisine from recommendations
            const cuisines = data.recommendations
                .map(item => item.cuisine_type)
                .filter(cuisine => cuisine);
                
            if (cuisines.length > 0) {
                // Find most frequent cuisine
                const cuisineCount = {};
                cuisines.forEach(cuisine => {
                    cuisineCount[cuisine] = (cuisineCount[cuisine] || 0) + 1;
                });
                
                detectedCuisine = Object.keys(cuisineCount).reduce((a, b) => 
                    cuisineCount[a] > cuisineCount[b] ? a : b
                );
            }
        }
        
        // If no cuisine detected, try to infer from current query
        if (!detectedCuisine && this.currentQuery) {
            const queryLower = this.currentQuery.toLowerCase();
            if (queryLower.includes('italian') || queryLower.includes('pizza') || queryLower.includes('pasta')) {
                detectedCuisine = 'Italian';
            } else if (queryLower.includes('indian') || queryLower.includes('curry') || queryLower.includes('biryani')) {
                detectedCuisine = 'Indian';
            } else if (queryLower.includes('chinese') || queryLower.includes('noodles') || queryLower.includes('dim sum')) {
                detectedCuisine = 'Chinese';
            } else if (queryLower.includes('mexican') || queryLower.includes('taco') || queryLower.includes('burrito')) {
                detectedCuisine = 'Mexican';
            } else if (queryLower.includes('american') || queryLower.includes('burger') || queryLower.includes('bbq')) {
                detectedCuisine = 'American';
            } else if (queryLower.includes('mediterranean') || queryLower.includes('greek') || queryLower.includes('olive')) {
                detectedCuisine = 'Mediterranean';
            } else if (queryLower.includes('japanese') || queryLower.includes('sushi') || queryLower.includes('ramen')) {
                detectedCuisine = 'Japanese';
            } else if (queryLower.includes('thai') || queryLower.includes('pad thai') || queryLower.includes('tom yum')) {
                detectedCuisine = 'Thai';
            } else if (queryLower.includes('french') || queryLower.includes('croissant') || queryLower.includes('baguette')) {
                detectedCuisine = 'French';
            } else if (queryLower.includes('korean') || queryLower.includes('kimchi') || queryLower.includes('bulgogi')) {
                detectedCuisine = 'Korean';
            } else if (queryLower.includes('spicy') || queryLower.includes('hot')) {
                detectedCuisine = 'Spicy'; // Special category for spicy food
            } else if (queryLower.includes('vegetarian') || queryLower.includes('vegan') || queryLower.includes('plant')) {
                detectedCuisine = 'Vegetarian'; // Special category for vegetarian
            }
        }
        
        // Return cuisine-specific emoji or special category emojis
        const cuisineEmojis = {
            'Italian': '🍝',
            'Indian': '🍛', 
            'Chinese': '🥢',
            'Mexican': '🌮',
            'American': '🍔',
            'Mediterranean': '🫒',
            'Japanese': '🍱',
            'Thai': '🍜',
            'French': '🥐',
            'Greek': '🥙',
            'Korean': '🍲',
            'Vietnamese': '🍜',
            'Spanish': '🥘',
            'Turkish': '🥙',
            'Lebanese': '🧆',
            'Spicy': '🌶️',
            'Vegetarian': '🥬',
            'Dessert': '🍰',
            'Seafood': '🦐',
            'BBQ': '🍖'
        };
        
        return cuisineEmojis[detectedCuisine] || '🍽️';
    }

    startCookingMessages() {
        const messages = [
            "Cooking up your perfect recommendations...",
            "Simmering through the best dishes...",
            "Seasoning the results with AI magic...",
            "Plating the finest selections...",
            "Adding the final touches...",
            "Almost ready to serve!"
        ];
        
        const loadingMessage = document.getElementById('loadingMessage');
        let messageIndex = 0;
        
        // Clear any existing interval
        if (this.loadingInterval) {
            clearInterval(this.loadingInterval);
        }
        
        // Cycle through messages every 1.5 seconds
        this.loadingInterval = setInterval(() => {
            messageIndex = (messageIndex + 1) % messages.length;
            if (loadingMessage) {
                loadingMessage.style.opacity = '0';
                setTimeout(() => {
                    loadingMessage.textContent = messages[messageIndex];
                    loadingMessage.style.opacity = '1';
                }, 200);
            }
        }, 1500);
    }

    toggleChat(show){
      const el = document.getElementById('chatWidget');
      el.style.display = show ? 'block' : 'none';
      if(show) document.getElementById('chatInput').focus();
    }

    appendChat(role, content){
      const box = document.getElementById('chatMessages');
      const div = document.createElement('div');
      div.className = `msg ${role}`;
      div.innerHTML = this.formatAIResponse(content);
      box.appendChild(div);
      box.scrollTop = box.scrollHeight;
    }

    setTyping(on){
      const box = document.getElementById('chatMessages');
      let t = box.querySelector('.msg.typing');
      if(on){
        if(!t){
          t = document.createElement('div');
          t.className = 'msg assistant typing';
          t.textContent = 'Assistant is typing... 🍳';
          box.appendChild(t);
        }
      }else if(t){
        t.remove();
      }
      box.scrollTop = box.scrollHeight;
    }

    async sendChatMessage(textOverride) {
        const input = document.getElementById('chatInput');
        const text = (textOverride ?? input.value).trim();
        if (!text) return;

        if (!textOverride) input.value = '';
        this.appendChat('user', text);
        this.setTyping(true);

        try {
            const res = await fetch('/chat', {
                method:'POST',
                headers:{'Content-Type':'application/json'},
                body: JSON.stringify({ session_id: this.chatSessionId || null, message: text })
            });
            const data = await res.json();
            if (!this.chatSessionId && data.session_id) {
                this.chatSessionId = data.session_id;
                localStorage.setItem('sweetpick_chat_session', this.chatSessionId);
            }
            this.setTyping(false);
            this.appendChat('assistant', data.natural_response || 'Here are some ideas!');
            if (data.recommendations?.length) {
                this.displayResults({
                    recommendations: data.recommendations,
                    processing_time: data.processing_time,
                    natural_response: data.natural_response
                });
            }
        } catch(e) {
            this.setTyping(false);
            this.appendChat('assistant', 'Sorry, something went wrong. Please try again.');
        }
    }

    // 2) Only use chat for follow-up questions, not initial queries
    shouldUseChat(text) {
        if (!text) return false;
        
        // Don't redirect to chat for initial queries - let them go through normal search
        // Only use chat for explicit follow-up requests
        const shouldChat = text.toLowerCase().includes('follow up') ||
                          text.toLowerCase().includes('more questions') ||
                          text.toLowerCase().includes('continue in chat') ||
                          text.toLowerCase().includes('chat mode');
        
        // Debug logging
        console.log(`shouldUseChat("${text}") = ${shouldChat}`);
        
        return shouldChat;
    }

    // 4) Open chat prefilled with current context
    openChatWithContext() {
        this.toggleChat(true);
        const input = document.getElementById('chatInput');
        if (input) {
            input.value = this.currentQuery
                ? `Follow-up about "${this.currentQuery}": `
                : `I have a follow-up question: `;
            input.focus();
        }
    }

    // Add helpers (near other utils)
    formatDishName(name) {
      if (!name) return '';
      return name
        .replace(/\s+/g, ' ')
        .trim()
        .split(' ')
        .map(w => w.charAt(0).toUpperCase() + w.slice(1))
        .join(' ');
    }

    renderDishBadges(item) {
      const dishes = Array.isArray(item.dishes)
        ? item.dishes
        : (item.dish_name
            ? item.dish_name.split(/,| and /i).map(s => s.trim()).filter(Boolean)
            : []);
      if (!dishes.length) return '';
      return `
        <div class="dish-badges">
          ${dishes.slice(0, 3).map(d => `<span class="dish-badge">🍽️ ${this.formatDishName(d)}</span>`).join('')}
        </div>
      `;
    }
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.sweetPickApp = new SweetPickApp();
});

// Add smooth scrolling for better UX
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            target.scrollIntoView({
                behavior: 'smooth',
                block: 'start'
            });
        }
    });
});