import pandas as pd

ap_cols = ['ID', 'Name', 'City', 'Country', 'IATA', 'ICAO', 'Latitude', 'Longitude', 'Altitude', 'Timezone', 'DST', 'Tz', 'Type', 'Source']
ap = pd.read_csv('airports.dat', names=ap_cols)

route_cols = ['Airline', 'AirlineID', 'Source', 'SourceID', 'Destination', 'DestinationID', 'Codeshare', 'Stops', 'Equipment']
dest = pd.read_csv('routes.dat', names=route_cols)[['Destination']].groupby('Destination').size().reset_index(name='count').sort_values(by='Destination')
dest.columns = ['IATA', 'count']

# Number of airport
print('Number of airport:', ap['Name'].unique().size)

# Number of airport each country
ap_each_country = ap[['Country']].groupby('Country').size().reset_index(name='count').sort_values(by='count', ascending=False)
print('Number of airport each country:\n', ap_each_country)

# Group by country
by_country = ap.groupby('Country')

# Filter by country
for c in by_country:
    print(f'\n\nFilter by country {c[0]}:')
    print(c[1].drop(columns='Country'))

# Number of flights arriving in each country
ap = ap[['Country', 'IATA']]
ap = ap[ap['IATA'] != '\\N'].merge(dest)[['Country', 'count']].sort_values(by='Country').groupby('Country').sum().reset_index()
print('\n\nNumber of flights arriving in each country:\n', ap)