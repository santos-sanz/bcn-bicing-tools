from analysis.utils_local import get_station_information, district_scores, district_mapping
import pandas as pd
import numpy as np

# Get station information
df = get_station_information()

# Calculate average altitude by district weighted by capacity, handling NaN values
altitude_by_district = df.groupby('district_code').apply(
    lambda x: np.average(x['altitude'].fillna(x['altitude'].mean()), weights=x['capacity'])
).reset_index(name='altitude')

# Calculate simple average altitude by district for comparison
simple_altitude_by_district = df.groupby('district_code')['altitude'].mean().reset_index()
simple_altitude_by_district = simple_altitude_by_district.rename(columns={'altitude': 'altitude_simple'})

# Get satisfaction scores for all available years
years = ['2019', '2020', '2021', '2022', '2023']
correlations = []
correlations_simple = []

for year in years:
    # Create satisfaction scores dataframe for current year with district codes
    satisfaction_scores = pd.DataFrame({
        'district_name': district_scores.keys(),
        'district_code': [district_mapping[district] for district in district_scores.keys()],
        'satisfaction': [scores[year] for scores in district_scores.values()]
    })
    
    # Merge altitude and satisfaction data
    correlation_df = pd.merge(altitude_by_district, satisfaction_scores, on='district_code')
    correlation_df = pd.merge(correlation_df, simple_altitude_by_district, on='district_code')
    
    # Calculate correlation coefficients
    correlation = correlation_df['altitude'].corr(correlation_df['satisfaction'])
    correlation_simple = correlation_df['altitude_simple'].corr(correlation_df['satisfaction'])
    correlations.append({'year': year, 'correlation': correlation})
    correlations_simple.append({'year': year, 'correlation': correlation_simple})

print("Correlation Analysis between Station Altitude and User Satisfaction by District")
print("-" * 70)
print("\nCorrelations by year:")
print("-" * 50)
print(f"{'Year':^10} | {'Simple':^15} | {'Weighted':^15}")
print("-" * 50)
for c, cs in zip(correlations, correlations_simple):
    print(f"{c['year']:^10} | {cs['correlation']:^15.3f} | {c['correlation']:^15.3f}")
print("-" * 50)

print("\nDetailed data for latest year (2023):")
print(correlation_df[['district_code', 'district_name', 'altitude', 'altitude_simple', 'satisfaction']].sort_values('district_code'))
