"""
Scraper Module - Adapted from Phase 1
Scrapes Google Play Store reviews and preprocesses them
"""

import pandas as pd
from google_play_scraper import app, reviews, Sort
import time
from datetime import datetime, timedelta, timezone
import re
import hashlib
import sys
import io
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Fix encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# ========================
# HELPER FUNCTIONS
# ========================

def safe_get(dictionary, key, default=''):
    """Safely extract values"""
    try:
        return dictionary.get(key, default)
    except:
        return default

def generate_hash_id(text, timestamp, username):
    """Fallback unique ID generator"""
    combined = f"{text}_{timestamp}_{username}"
    return hashlib.md5(combined.encode()).hexdigest()[:16]

def count_urdu_words(text):
    """Count Roman Urdu words"""
    urdu_words = ['hai', 'nahi', 'acha', 'bohat', 'kya', 'app', 'bahut', 
                  'achha', 'zabardast', 'mast', 'bakwas', 'bekar', 'kharab',
                  'theek', 'sahi', 'galat', 'mazay', 'bewakoof']
    if not text:
        return 0
    text_lower = text.lower()
    return sum(1 for word in urdu_words if word in text_lower)

def check_generic_username(username):
    """Check if username is generic"""
    if not username:
        return 1
    pattern = r'^(user|reviewer|test|account|google|android|person|member)\d*$'
    return 1 if re.match(pattern, username.strip(), re.IGNORECASE) else 0

def count_promo_words(text):
    """Count promotional keywords"""
    promo_keywords = ['download', 'free', 'buy now', 'click here', 'visit', 
                     'limited time', 'offer', 'discount', 'deal', 'sale',
                     'cheap', 'install', 'link', 'website', 'promocode']
    if not text:
        return 0
    text_lower = text.lower()
    return sum(1 for keyword in promo_keywords if keyword in text_lower)

def is_valid_review(text, max_words=None):
    """Quality filter for scraping"""
    if not text or len(text.strip()) < 5:
        return False
    
    word_count = len(text.split())
    if max_words and word_count > max_words:
        return False
    
    url_count = text.count('http') + text.count('www.') + text.count('.com')
    if url_count > 2:
        return False
    
    return True

def compute_sentiment_score(text):
    """Compute sentiment score using VADER"""
    analyzer = SentimentIntensityAnalyzer()
    if not text:
        return 0.0
    scores = analyzer.polarity_scores(text)
    return scores['compound']

# ========================
# SCRAPING FUNCTIONS
# ========================

def search_app_by_name(app_name):
    """Search for app by name and return app ID"""
    # This is a simplified version - in production, you'd use search API
    # For now, we'll try common app IDs or ask user for app ID
    common_apps = {
        'whatsapp': 'com.whatsapp',
        'instagram': 'com.instagram.android',
        'daraz': 'com.daraz.android',
        'amazon': 'com.amazon.mShop.android.shopping',
        'foodpanda': 'com.global.foodpanda.android',
        'careem': 'com.careem.acma',
        'jazzcash': 'com.techlogix.mobilinkcustomer',
        'easypaisa': 'pk.com.telenor.phoenix',
        'netflix': 'com.netflix.mediaclient',
        'spotify': 'com.spotify.music'
    }
    
    app_name_lower = app_name.lower().strip()
    if app_name_lower in common_apps:
        return common_apps[app_name_lower]
    
    # If not found, return None (user will need to provide app ID)
    return None

def get_app_info(app_id):
    """Get app metadata"""
    try:
        info = app(app_id)
        return {
            'app_id': app_id,
            'app_name': safe_get(info, 'title'),
            'category': safe_get(info, 'genre'),
            'rating': safe_get(info, 'score', 0),
            'total_reviews': safe_get(info, 'reviews', 0),
            'installs': safe_get(info, 'installs'),
            'developer': safe_get(info, 'developer', 'Unknown')
        }
    except Exception as e:
        print(f"Error getting app info: {e}")
        return None

def _normalize_timestamp(value):
    """Convert timestamps (datetime, pandas Timestamp) to naive UTC for comparison."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if hasattr(value, "tzinfo") and value.tzinfo:
        value = value.astimezone(timezone.utc)
    return value.replace(tzinfo=None)


def scrape_reviews(
    app_id,
    app_name,
    max_reviews=None,
    days_back=None,
    rating_filter=None,
    progress_callback=None,
    review_callback=None,
    status_callback=None
):
    """
    Scrape reviews from Google Play Store with filters
    
    Args:
        app_id: App ID
        app_name: App name
        max_reviews: Maximum number of reviews to collect (None = no limit, collect all available)
        days_back: Number of days back to filter (None = all time)
        rating_filter: Rating to filter (1-5, None = all ratings)
    """
    all_reviews = []
    continuation_token = None
    
    # Calculate date filter
    date_filter = None
    if days_back:
        date_filter = datetime.utcnow() - timedelta(days=days_back)
    
    intro = f"Scraping reviews for {app_name}..."
    print(intro)
    if status_callback:
        status_callback(intro)
    if days_back:
        msg = f"Filter: Last {days_back} days"
        print(f"   {msg}")
        status_callback and status_callback(msg)
    if rating_filter:
        msg = f"Filter: {rating_filter} star rating"
        print(f"   {msg}")
        status_callback and status_callback(msg)
    if max_reviews:
        if days_back:
            reviews_per_day = max_reviews / max(days_back, 1)
            msg = f"Target: {max_reviews} total reviews (~{reviews_per_day:.1f} per day)"
        else:
            msg = f"Target: {max_reviews} total reviews"
    else:
        msg = "Target: All available reviews (no limit)"
    print(f"   {msg}")
    if status_callback:
        status_callback(msg)
    
    iteration = 0
    no_new_reviews_count = 0
    max_iterations_without_new = 5  # Stop after 5 iterations with no new reviews
    date_window_exhausted = False
    
    # Continue until limit reached or no more reviews
    while True:
        # Check if we've reached the limit
        if max_reviews and len(all_reviews) >= max_reviews:
            print(f"   Reached target of {max_reviews} reviews")
            break
        try:
            iteration += 1
            
            # Prefer newest-first when time window is specified 
            if days_back:
                sort_methods = [Sort.NEWEST]
            else:
                sort_methods = [Sort.MOST_RELEVANT, Sort.NEWEST, Sort.RATING]
            sort_method = sort_methods[iteration % len(sort_methods)]
            
            result, continuation_token = reviews(
                app_id,
                lang='en',
                country='pk',
                sort=sort_method,
                count=200,  # Get more per request
                continuation_token=continuation_token
            )
            
            if not result:
                no_new_reviews_count += 1
                if no_new_reviews_count >= max_iterations_without_new:
                    msg = f"No more reviews available after {max_iterations_without_new} attempts"
                    print(f"   {msg}")
                    status_callback and status_callback(msg)
                    break
                time.sleep(2)
                continue
            
            no_new_reviews_count = 0
            added_this_iter = 0
            
            # Filter reviews by date and rating
            for review in result:
                # Check if we have enough (only if limit is set)
                if max_reviews and len(all_reviews) >= max_reviews:
                    break
                
                # Check rating filter
                review_rating = safe_get(review, 'score', 0)
                if rating_filter and review_rating != rating_filter:
                    continue
                
                # Check date filter
                review_date = safe_get(review, 'at', None)
                if date_filter and review_date:
                    review_dt = _normalize_timestamp(review_date)
                    if review_dt and review_dt < date_filter:
                        date_window_exhausted = True
                        continue  # Skip old reviews
                
                # Check if valid
                text = safe_get(review, 'content', '')
                if is_valid_review(text):
                    all_reviews.append(review)
                    added_this_iter += 1
                    if review_callback:
                        try:
                            review_callback(review, len(all_reviews))
                        except Exception as cb_exc:
                            print(f"   Warning: review callback failed: {cb_exc}")
            
            # Progress message
            if max_reviews:
                iter_msg = f"Iteration {iteration}: Collected {len(all_reviews)}/{max_reviews} reviews (added {added_this_iter} this batch)"
            else:
                iter_msg = f"Iteration {iteration}: Collected {len(all_reviews)} reviews (added {added_this_iter} this batch)"
            print(f"   {iter_msg}")
            if status_callback:
                status_callback(iter_msg)
            if progress_callback:
                try:
                    progress_callback(
                        len(all_reviews),
                        max_reviews,
                        iteration,
                        added_this_iter,
                        final=False
                    )
                except Exception as cb_exc:
                    print(f"   Warning: progress callback failed: {cb_exc}")
            
            if date_window_exhausted:
                msg = "Reached end of selected date window"
                print(f"   {msg}")
                status_callback and status_callback(msg)
                break

            if not continuation_token:
                msg = "No continuation token, stopping"
                print(f"   {msg}")
                status_callback and status_callback(msg)
                break
            
            # If we didn't add any reviews this iteration, increment counter
            if added_this_iter == 0:
                no_new_reviews_count += 1
                if no_new_reviews_count >= max_iterations_without_new:
                    msg = f"No new reviews found after {max_iterations_without_new} iterations, stopping"
                    print(f"   {msg}")
                    status_callback and status_callback(msg)
                    break
                time.sleep(1)
                continue
            else:
                no_new_reviews_count = 0  # Reset counter if we found reviews
                
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            msg = f"Warning: {e}"
            print(f"   {msg}")
            status_callback and status_callback(msg)
            time.sleep(2)
            continue
    
    final_msg = f"Final: Collected {len(all_reviews)} reviews"
    print(f"   {final_msg}")
    if status_callback:
        status_callback(final_msg)
    if progress_callback:
        try:
            progress_callback(len(all_reviews), max_reviews, iteration, 0, final=True)
        except Exception as cb_exc:
            print(f"   Warning: final progress callback failed: {cb_exc}")
    return all_reviews

# ========================
# PREPROCESSING
# ========================

def process_reviews(reviews_data, app_name, category='Unknown'):
    """Process and flag reviews (Phase 1 logic)"""
    processed = []
    
    for review in reviews_data:
        # Basic extraction
        review_id = safe_get(review, 'reviewId', '')
        username = safe_get(review, 'userName', 'Anonymous')
        timestamp = safe_get(review, 'at', '')
        text_raw = safe_get(review, 'content', '')
        device = safe_get(review, 'reviewDevice', 'Unknown')
        rating = safe_get(review, 'score', 0)
        thumbs_up = safe_get(review, 'thumbsUpCount', 0)
        
        # Fallback ID if missing
        if not review_id:
            review_id = generate_hash_id(text_raw, str(timestamp), username)
        
        # Keep raw + cleaned text
        text_original = text_raw
        text_cleaned = text_raw.lower().strip() if text_raw else ''
        
        # Text features
        token_count = len(text_cleaned.split())
        char_count = len(text_cleaned)
        num_emojis = len([c for c in text_cleaned if ord(c) > 127000])
        num_urls = text_cleaned.count('http') + text_cleaned.count('www.')
        num_uppercase_words = len([w for w in text_raw.split() if w.isupper() and len(w) > 1])
        
        words = text_cleaned.split()
        unique_word_ratio = len(set(words)) / max(len(words), 1)
        
        # Flags
        contains_promo_words = 1 if count_promo_words(text_cleaned) > 0 else 0
        has_urdu_words = 1 if count_urdu_words(text_cleaned) > 0 else 0
        sentiment_score = compute_sentiment_score(text_cleaned)
        device_missing = 1 if device == 'Unknown' or not device else 0
        generic_username = check_generic_username(username)
        
        processed.append({
            'review_id': review_id,
            'app_name': app_name,
            'category': category,
            'username': username,
            'device': device,
            'rating': rating,
            'timestamp': timestamp,
            'text_original': text_original,
            'text_cleaned': text_cleaned,
            'thumbs_up': thumbs_up,
            'token_count': token_count,
            'char_count': char_count,
            'num_emojis': num_emojis,
            'num_urls': num_urls,
            'num_uppercase_words': num_uppercase_words,
            'unique_word_ratio': unique_word_ratio,
            'contains_promo_words': contains_promo_words,
            'has_urdu_words': has_urdu_words,
            'sentiment_score': sentiment_score,
            'device_missing': device_missing,
            'generic_username': generic_username
        })
    
    df = pd.DataFrame(processed)
    
    # Parse timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Behavioral flags (user-level)
    user_counts = df['username'].value_counts().to_dict()
    df['user_total_reviews'] = df['username'].map(user_counts)
    
    df['time_diff_seconds'] = df.groupby('username')['timestamp'].diff().dt.total_seconds()
    df['time_diff_seconds'] = df['time_diff_seconds'].fillna(0)
    
    df['is_burst'] = ((df['time_diff_seconds'] > 0) & 
                      (df['time_diff_seconds'] < 300)).astype(int)
    
    text_counts = df['text_cleaned'].value_counts().to_dict()
    df['same_text_count'] = df['text_cleaned'].map(text_counts)
    
    df['rating_text_mismatch'] = (
        ((df['sentiment_score'] <= -0.5) & (df['rating'] >= 4)) |
        ((df['sentiment_score'] >= 0.5) & (df['rating'] <= 2))
    ).astype(int)
    
    user_rating_mean = df.groupby('username')['rating'].mean().to_dict()
    user_rating_std = df.groupby('username')['rating'].std().fillna(0).to_dict()
    df['user_avg_rating'] = df['username'].map(user_rating_mean)
    df['user_rating_std'] = df['username'].map(user_rating_std)
    
    return df

# ========================
# MAIN FUNCTION
# ========================

def scrape_and_preprocess(
    app_name_or_id,
    max_reviews=None,
    category='Unknown',
    days_back=None,
    rating_filter=None,
    progress_callback=None,
    review_callback=None,
    status_callback=None
):
    """
    Main function to scrape and preprocess reviews
    
    Args:
        app_name_or_id: App name (will search) or app ID (direct)
        max_reviews: Maximum number of reviews to scrape
        category: App category
        days_back: Number of days back to filter (None = all time)
        rating_filter: Rating to filter (1-5, None = all ratings)
    
    Returns:
        DataFrame with preprocessed reviews
    """
    # Try to get app ID
    app_id = search_app_by_name(app_name_or_id)
    if not app_id:
        # Assume it's already an app ID
        app_id = app_name_or_id
    
    # Get app info
    app_info = get_app_info(app_id)
    if app_info:
        app_name = app_info['app_name']
        category = app_info.get('category', category)
    else:
        app_name = app_name_or_id
    
    # Scrape reviews
    reviews_data = scrape_reviews(
        app_id,
        app_name,
        max_reviews,
        days_back,
        rating_filter,
        progress_callback=progress_callback,
        review_callback=review_callback,
        status_callback=status_callback
    )
    
    if not reviews_data:
        raise ValueError(f"No reviews found for {app_name}")
    
    # Process reviews
    df = process_reviews(reviews_data, app_name, category)
    
    return df

