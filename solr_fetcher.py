import httpx
import json
import time
from typing import Dict, Any, List, Optional


class SolrBatchFetcher:
    def __init__(self, base_url: str, core_name: str, batch_size: int = 1000):
        self.base_url = base_url.rstrip('/')
        self.core_name = core_name
        self.batch_size = batch_size
        self.query_url = f"{self.base_url}/{self.core_name}/select"

    def fetch_all(self,
                  query_params: Optional[Dict] = None,
                  fields: Optional[List[str]] = None,
                  sort: Optional[str] = None,
                  delay: float = 0.1) -> List[Dict[str, Any]]:
        """
        Fetch all records with custom parameters

        Args:
            query_params: Additional query parameters
            fields: List of fields to return (None returns all fields)
            sort: Sort order (e.g., 'id asc')
            delay: Delay between requests to avoid overwhelming the server
        """

        # Base parameters
        params = {
            'q': '*:*',
            'rows': self.batch_size,
            'wt': 'json',
            'indent': 'false'
        }

        # Add optional parameters
        if fields:
            params['fl'] = ','.join(fields)
        if sort:
            params['sort'] = sort
        if query_params:
            params.update(query_params)

        all_records = []
        start = 0

        while True:
            params['start'] = start

            try:
                response = httpx.get(self.query_url, params=params)
                response.raise_for_status()

                data = response.json()
                records = data.get('response', {}).get('docs', [])
                # print(json.dumps(records[0], indent=2))
                # exit()
                num_found = data.get('response', {}).get('numFound', 0)

                all_records.extend(records)

                print(f"Fetched {len(records)} records (Total: {len(all_records)}/{num_found})")

                if len(records) < self.batch_size or len(all_records) >= num_found:
                    break

                start += self.batch_size
                time.sleep(delay)  # Be nice to the server

            except Exception as e:
                print(f"Error on batch starting at {start}: {e}")
                break

        return all_records

    def get_stats(self) -> Dict[str, Any]:
        """Get basic statistics about the core"""
        params = {
            'q': '*:*',
            'rows': 0,
            'wt': 'json'
        }

        try:
            response = httpx.get(self.query_url, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get('response', {})
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}


# Example usage
if __name__ == "__main__":
    # Configuration
    BASE_URL = "http://localhost:38983/solr"
    CORE_NAME = "omeka"

    # Create fetcher instance
    fetcher = SolrBatchFetcher(BASE_URL, CORE_NAME, batch_size=9000)

    # Get total record count
    stats = fetcher.get_stats()
    total_records = stats.get('numFound', 0)
    print(f"Total records in core: {total_records}")

    if total_records > 0:
        # Fetch all records
        print("\nFetching all records...")
        all_records = fetcher.fetch_all(
            fields=['id', 'identifier', 'subject', 'collector', 'creator', 'language', 'subgenre', 'main_text', 'description', 'date_start', 'latitude', 'longitude'],  # Specify fields to return
            sort='id asc',  # Sort by id
            delay=0.2  # 200ms delay between requests
        )

        # Save to file
        output_file = f"{CORE_NAME}_all_records.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_records, f, indent=2, ensure_ascii=False)

        print(f"\nSaved {len(all_records)} records to {output_file}")

        # Show sample
        if all_records:
            print("\nSample record:")
            print(json.dumps(all_records[0], indent=2, ensure_ascii=False))
