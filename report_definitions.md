# Report Definitions

This document provides platform-agnostic specifications for all analytics reports in this solution. Each report is described in terms of its visualizations, data elements, filters, and interactive behaviors to enable recreation on any business intelligence platform.

---

## Artists Last 24 Hours Report

### Purpose
Displays listening activity metrics for the most recent 24-hour period, providing a quick overview of total listening time, track diversity, and artist engagement.

### Data Sources
- **Primary Entity**: Kusto Query Result (listening activity data)
- **Key Attributes**: artist, track_name, minutes_played

### Page Layout

#### Visual 1: Total Minutes Card
**Type**: Key Performance Indicator (KPI) Card  
**Position**: Top-left section  
**Data Elements**:
- Measure: Sum of `minutes_played`
- Display Name: "total minutes"

**Aggregation**: Sum  
**Sort Order**: Descending by value  
**Visual Properties**:
- Border: Visible
- Data Label: Numeric value

**Purpose**: Shows the cumulative listening time in minutes for the last 24 hours.

---

#### Visual 2: Total Tracks Card
**Type**: Key Performance Indicator (KPI) Card  
**Position**: Middle-left section  
**Data Elements**:
- Dimension: Distinct count of `track_name`
- Display Name: "total tracks"

**Aggregation**: Min (distinct count equivalent)  
**Sort Order**: Descending by value  
**Visual Properties**:
- Border: Visible
- Data Label: Numeric value

**Purpose**: Displays the number of unique tracks played in the last 24 hours.

---

#### Visual 3: Total Artists Card
**Type**: Key Performance Indicator (KPI) Card  
**Position**: Bottom-left section  
**Data Elements**:
- Dimension: Distinct count of `artist`
- Display Name: "total artists"

**Aggregation**: Min (distinct count equivalent)  
**Sort Order**: Descending by value  
**Visual Properties**:
- Border: Visible
- Data Label: Numeric value

**Purpose**: Indicates the number of unique artists played in the last 24 hours.

---

#### Visual 4: Artists by Minutes Played Chart
**Type**: Horizontal Bar Chart  
**Position**: Right section (main visualization area)  
**Data Elements**:
- Category Axis: `artist`
- Value Axis: Sum of `minutes_played`
- Display Name for Measure: "minutes played"
- Series: `artist` (color-coded by artist)

**Aggregation**: Sum  
**Sort Order**: Descending by minutes played  
**Visual Properties**:
- Border: Visible
- Legend: Hidden
- Orientation: Horizontal bars
- Chart displays artists ranked by listening time

**Purpose**: Visualizes the distribution of listening time across artists, allowing quick identification of most-played artists in the period.

**Interactivity**:
- Drill-down filtering enabled for other visuals
- Click on artist to filter related data

---

## Tracks History Report

### Purpose
Comprehensive analysis of historical listening patterns across multiple dimensions including time of day, date ranges, artist genres, and geographic locations. Enables exploration of listening habits over time with drill-down capabilities.

### Data Sources
- **Fact Tables**: 
  - `fact_track_played` (track listening events)
  - `fact_artist_genre` (artist-genre relationships)
- **Dimension Tables**: 
  - `dim_time` (time attributes)
  - `dim_date` (date attributes)
  - `dim_artist` (artist details and location data)
  - `dim_artist_genre` (genre metadata)

### Global Filters
- **Artist Name Filter**: Excludes null values from `dim_artist.artist_name`
  - Applied across all pages
  - Inverted selection mode (exclude nulls)

---

## Page 1: Time of Day Analysis

### Purpose
Analyzes listening patterns by hour of day and time period (morning, afternoon, evening, night).

### Page-Level Filters
- **Country Code Filter**: Optional filter on `dim_artist.country_code`
  - Type: Categorical
  - Applied to page visuals

### Page Layout

#### Visual 1: Date Range Slicer
**Type**: Date Range Slicer  
**Position**: Top-left corner  
**Data Elements**:
- Dimension: `dim_date.date`

**Visual Properties**:
- Border: Visible
- Mode: Between (date range)
- Default Range: 2020-08-24 to 2025-08-20
- Sort Order: Ascending by date

**Purpose**: Allows users to filter all visuals on the page by selecting a date range.

**Interactivity**:
- Filters all other visuals on the page when date range is selected

---

#### Visual 2: Country Code Slicer
**Type**: Dropdown Slicer  
**Position**: Top-left section (below date slicer)  
**Data Elements**:
- Dimension: `dim_artist.country_code`

**Visual Properties**:
- Border: Visible
- Mode: Dropdown list
- Selection: Multiple values allowed

**Purpose**: Filters data by artist's country of origin.

**Interactivity**:
- Filters all visuals on page when country is selected
- Drill-down filtering enabled

---

#### Visual 3: Tracks Played by Year
**Type**: Horizontal Bar Chart  
**Position**: Left section (below slicers)  
**Data Elements**:
- Category Axis: `dim_date.year_num`
- Value Axis: `fact_track_played.Count Tracks Played` (measure)
- Display Title: "Count Tracks Played"

**Aggregation**: Count measure  
**Sort Order**: Ascending by year  
**Visual Properties**:
- Border: Visible
- Legend: Hidden
- Axis Type: Categorical
- X-Axis Title: Hidden
- Y-Axis Title: Hidden

**Purpose**: Shows the volume of tracks played for each year in the dataset.

**Interactivity**:
- Click on year to filter other visuals
- Cross-filters hour and time of day visualizations

---

#### Visual 4: Tracks by Hour of Day
**Type**: Vertical Column Chart  
**Position**: Top-right section  
**Data Elements**:
- Category Axis: `dim_time.hour_of_day`
- Value Axis: `fact_track_played.Count Tracks Played` (measure)
- Series: `dim_time.time_of_day` (for color coding)

**Filters**:
- Excludes null values from `dim_time.hour_of_day`

**Aggregation**: Count measure  
**Sort Order**: Ascending by hour (0-23)  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden
- Axis Type: Categorical
- X-Axis Title: Visible
- Displays all 24 hours

**Purpose**: Visualizes listening activity distribution across 24-hour clock, showing peak listening hours.

**Interactivity**:
- Click on hour to filter time of day visual
- Cross-filters with time of day and year visuals
- Drill-down filtering enabled

---

#### Visual 5: Tracks by Time of Day
**Type**: Vertical Column Chart  
**Position**: Bottom-right section  
**Data Elements**:
- Category Axis: `dim_time.time_of_day` (categorical: Morning, Afternoon, Evening, Night)
- Value Axis: `fact_track_played.Count Tracks Played` (measure)
- Series: `dim_time.time_of_day` (for color coding)

**Filters**:
- Excludes null values from `dim_time.hour_of_day`

**Aggregation**: Count measure  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden
- Axis Type: Categorical

**Purpose**: Aggregates listening activity into broad time periods for high-level pattern analysis.

**Interactivity**:
- Click on time period to filter hour of day visual
- Cross-filters with hourly and yearly visuals
- Drill-down filtering enabled

---

## Page 2: Artist Genre Analysis

### Purpose
Explores listening patterns through the lens of musical genres, showing genre distribution, top artists within genres, and historical trends.

### Page Layout

#### Visual 1: Date Range Slicer
**Type**: Date Range Slicer  
**Position**: Top-left corner  
**Data Elements**:
- Dimension: `dim_date.date`

**Visual Properties**:
- Border: Visible
- Mode: Between (date range)
- Default Range: Current Year
- Sort Order: Ascending by date

**Purpose**: Controls the time period for genre analysis.

**Interactivity**:
- Filters all visuals on the page

---

#### Visual 2: Tracks by Year
**Type**: Horizontal Bar Chart  
**Position**: Left section (below date slicer)  
**Data Elements**:
- Category Axis: `dim_date.year_num`
- Value Axis: `fact_artist_genre.Count Tracks Played in Genre` (measure)
- Display Title: "Count Tracks Played"

**Aggregation**: Count measure  
**Sort Order**: Ascending by year  
**Visual Properties**:
- Border: Visible
- Title: Custom text displayed
- Legend: Hidden
- Axis Type: Categorical

**Purpose**: Shows yearly distribution of tracks played colored by genre associations.

**Interactivity**:
- Filters genre treemap and artist bar chart when year selected

---

#### Visual 3: Genre Distribution Treemap
**Type**: Treemap  
**Position**: Top-right section  
**Data Elements**:
- Group: `dim_artist_genre.genre`
- Size/Value: `fact_artist_genre.Count Tracks Played in Genre` (measure)
- Tooltips:
  - `dim_artist_genre.top_artist_in_genre` (most listened artist in genre)
  - `dim_artist_genre.most_popular_artist_in_genre` (most popular artist by Spotify metrics)

**Filters**:
- Top 25 genres by track count
- Excludes null genre values

**Aggregation**: Count measure  
**Sort Order**: Descending by track count  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden
- Color-coded rectangles sized by track count

**Purpose**: Provides proportional view of listening distribution across musical genres with contextual artist information on hover.

**Interactivity**:
- Click on genre to filter artist bar chart
- Hover to see top artists in that genre
- Cross-filters with year and artist visuals

---

#### Visual 4: Artists in Selected Genre
**Type**: Horizontal Bar Chart  
**Position**: Bottom-right section  
**Data Elements**:
- Category Axis: `dim_artist.artist_name`
- Value Axis: `fact_artist_genre.Count Tracks Played in Genre` (measure)

**Filters**:
- Top 25 genres by track count (inherited from treemap selection)
- Excludes null genre values

**Aggregation**: Count measure  
**Sort Order**: Descending by track count  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden

**Purpose**: Details artist-level breakdown within selected genre(s), showing which artists contribute most to genre listening.

**Interactivity**:
- Responds to genre selection from treemap
- Responds to year selection from bar chart
- Drill-down filtering enabled

---

## Page 3: Geographic Analysis

### Purpose
Maps listening activity to artist geographic origins, showing regional patterns and combining with genre and artist-level data.

### Page Layout

#### Visual 1: Date Range Slicer
**Type**: Date Range Slicer  
**Position**: Top-left corner  
**Data Elements**:
- Dimension: `dim_date.date`

**Visual Properties**:
- Border: Visible
- Mode: Between (date range)
- Default Range: Current Year

**Purpose**: Controls the time period for geographic analysis.

**Interactivity**:
- Filters all visuals on the page

---

#### Visual 2: Count Tracks Played Card
**Type**: Key Performance Indicator (KPI) Card  
**Position**: Left section below date slicer  
**Data Elements**:
- Measure: `fact_track_played.Count Tracks Played`

**Aggregation**: Count measure  
**Sort Order**: Descending by value  
**Visual Properties**:
- Border: Visible
- Font Size: 10pt for category label

**Purpose**: Displays total track count for the selected time period and geographic filters.

**Interactivity**:
- Updates based on all filter selections

---

#### Visual 3: Continent Slicer
**Type**: Multi-Select Slicer  
**Position**: Top-center section  
**Data Elements**:
- Dimension: `dim_artist.location_hierarchy.continent` (hierarchy level)

**Filters**:
- Excludes null/blank continent values

**Visual Properties**:
- Border: Visible
- Mode: Basic (checkbox list)

**Purpose**: Filters data by continental regions where artists originate.

**Interactivity**:
- Filters map, country slicer, and all other visuals
- Hierarchical drill-down to country level

---

#### Visual 4: Country Slicer
**Type**: Multi-Select Slicer  
**Position**: Center section (below continent slicer)  
**Data Elements**:
- Dimension: `dim_artist.location_hierarchy.country` (hierarchy level)

**Filters**:
- Responds to continent selection

**Visual Properties**:
- Border: Visible
- Mode: Basic (checkbox list)

**Purpose**: Provides country-level geographic filtering within selected continents.

**Interactivity**:
- Filters map and artist bar chart
- Responds to continent slicer selection
- Hierarchical drill-down enabled

---

#### Visual 5: Geographic Map
**Type**: Bubble Map (Azure Map)  
**Position**: Lower-left section (large main visual)  
**Data Elements**:
- Location: 
  - Latitude: Average of `dim_artist.lat`
  - Longitude: Average of `dim_artist.long`
- Size: `fact_track_played.Count Tracks Played` (measure)
- Series/Color: `dim_artist.location_hierarchy.country`
- Tooltips:
  - `dim_artist.location_hierarchy.state_province` (first value)
  - `dim_artist.location_hierarchy.city` (first value)

**Filters**:
- Excludes null country values
- Responds to continent and country slicer selections

**Aggregation**: 
- Count measure for bubble size
- Average for coordinates

**Sort Order**: Descending by track count  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden
- Map Style: Road map
- Style Picker: Hidden
- Navigation Controls: Hidden
- Selection Control: Hidden
- Bubble Properties:
  - Base radius: 9
  - Min radius: 9
  - Max radius: 29
  - Marker type: Magnitude-based
  - Stroke width: 1
  - Auto stroke color: Enabled
  - Clustering: Disabled

**Purpose**: Visualizes the geographic distribution of listened artists using proportionally-sized bubbles on a map, allowing spatial pattern analysis.

**Interactivity**:
- Click on bubble to filter artist bar chart
- Hover to see location details (state/province, city)
- Responds to continent and country filters
- Cross-filters with other visuals
- Drill-down filtering enabled

---

#### Visual 6: Primary Genre Treemap
**Type**: Treemap  
**Position**: Top-right section  
**Data Elements**:
- Group: `dim_artist.primary_genre`
- Size/Value: `fact_track_played.Count Tracks Played` (measure)

**Filters**:
- Top 20 genres by track count
- Excludes "no genre defined" values

**Aggregation**: Count measure  
**Sort Order**: Descending by track count  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Color-coded rectangles sized by track count

**Purpose**: Shows genre distribution for the selected geographic region(s).

**Interactivity**:
- Click on genre to filter artist bar chart
- Responds to geographic and date filters
- Cross-filters with map and artist chart

---

#### Visual 7: Artists by Track Count
**Type**: Horizontal Bar Chart  
**Position**: Bottom-right section  
**Data Elements**:
- Category Axis: `dim_artist.artist_name`
- Value Axis: `fact_track_played.Count Tracks Played` (measure)

**Aggregation**: Count measure  
**Sort Order**: Descending by track count  
**Visual Properties**:
- Border: Visible
- Title: Hidden
- Legend: Hidden
- Category Axis: Visible
- Axis Title: Hidden

**Purpose**: Lists individual artists ranked by listening volume within selected geographic and genre filters.

**Interactivity**:
- Responds to all filter selections (date, continent, country, genre)
- Drill-down filtering enabled

---

## Cross-Page Interactions

### Time of Day Analysis Page
- **Visual Relationships**: Bidirectional filtering between hour, time of day, and year visuals
- **Slicer Behavior**: Date and country slicers apply to all page visuals
- **Drill-Through**: Not explicitly configured but standard drill-down enabled

### Artist Genre Analysis Page
- **Visual Relationships**: 
  - Genre treemap filters artist bar chart
  - Year selection filters both genre and artist visuals
- **Slicer Behavior**: Date slicer applies to all page visuals
- **Drill-Through**: Not explicitly configured

### Geographic Analysis Page
- **Visual Relationships**: 
  - Continent filters country slicer and all visuals
  - Country filters map and artist visuals
  - Map selection filters artist bar chart
  - Genre treemap filters artist bar chart
- **Hierarchical Navigation**: Location hierarchy supports drill-down from continent → country → state/province → city
- **Slicer Behavior**: Date slicer applies to all page visuals

---

## Data Model Considerations

### Key Relationships
When implementing on other platforms, ensure these relationships are established:

1. **Date Dimension**:
   - `dim_date.date` relates to fact tables on played_date
   
2. **Time Dimension**:
   - `dim_time` relates to fact tables on played_time
   - Contains derived attributes: `hour_of_day`, `time_of_day`

3. **Artist Dimension**:
   - `dim_artist.artist_id` relates to fact tables
   - Contains location attributes: `country_code`, `lat`, `long`
   - Contains location hierarchy: `continent`, `country`, `state_province`, `city`
   - Contains genre attribute: `primary_genre`

4. **Genre Dimension**:
   - `dim_artist_genre.genre_id` relates to `fact_artist_genre`
   - Contains aggregated attributes: `top_artist_in_genre`, `most_popular_artist_in_genre`

5. **Fact Tables**:
   - `fact_track_played`: Grain is individual track play event
   - `fact_artist_genre`: Grain is artist-genre-time combination

### Measures
The following measures should be implemented:

1. **Count Tracks Played**: Count of records in `fact_track_played`
2. **Count Tracks Played in Genre**: Count of records in `fact_artist_genre`
3. **Sum of Minutes Played**: Sum of `minutes_played` from fact table

---

## Implementation Notes

### Filter Behavior
- All filters use inverted selection mode to exclude null values
- TopN filters (25 genres, 20 genres) should maintain dynamic ranking
- Date range slicers use "between" mode with both start and end dates

### Visual Styling
- All visualizations use consistent border styling
- Legends are typically hidden to maximize data display space
- Titles are hidden on most charts to reduce visual clutter
- Theme: CY23SU11 (can be substituted with platform-specific theme)

### Performance Considerations
- Genre filters use TopN to limit data volume (25 or 20 top genres)
- Geographic map uses average coordinates for performance
- Consider implementing aggregated views for historical year-over-year analysis

### Accessibility
- Ensure color schemes provide sufficient contrast
- Provide alternative text descriptions for visuals
- Support keyboard navigation for interactive elements
- Consider adding data labels for screen reader accessibility

---

## Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-10 | 1.0 | Initial documentation of artists_last_24_hours and tracks_history reports |
