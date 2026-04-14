import json
import argparse
import datetime
import traceback
from os import environ

import pandas as pd
from tqdm import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


class ElasticsearchDataExporter:
    def __init__(self, scheme, host, port):
        """
        Initialize Elasticsearch connection

        Args:
            hosts: List of Elasticsearch hosts
            http_auth: Authentication tuple (username, password)
        """
        # self.es = Elasticsearch(
        #     hosts=hosts,
        #     http_auth=http_auth,
        #     max_retries=10,
        #     retry_on_timeout=True
        # )
        self.es = Elasticsearch([{"scheme": scheme, "host": host, "port": port}])

        print(f"🔌 Connecting to Elasticsearch... {scheme=} {host=} {port=}")

        # Test connection
        if self.es.info():
            print("✅ Connected to Elasticsearch")
        else:
            raise ConnectionError("❌ Could not connect to Elasticsearch")

    def get_indices(self, pattern="*"):
        """Get list of indices matching pattern"""
        indices = list(self.es.indices.get_alias(index=pattern).keys())
        print(f"Found {len(indices)} indices: {indices}")
        return indices

    def get_total_count(self, index_name):
        """Get total document count for an index"""
        count = self.es.count(index=index_name)['count']
        print(f"📊 Index '{index_name}' has {count:,} documents")
        return count

    def get_all_documents_scroll(self, index_name, query=None, scroll_time="10m",
                                 size=1000, preserve_order=False, fields=None):
        """
        Retrieve all documents using Scroll API (recommended for large indices)

        Args:
            index_name: Name of the index
            query: Elasticsearch query (default: match_all)
            scroll_time: Scroll context keep-alive time
            size: Batch size per scroll request
            preserve_order: Whether to preserve order (uses search_after)
            fields: List of specific fields to retrieve

        Returns:
            List of documents
        """
        if query is None:
            query = {"query": {"match_all": {}}}

        total_docs = self.get_total_count(index_name)

        # Configure source filtering
        if fields:
            query["_source"] = fields

        print(f"🚀 Fetching all documents from '{index_name}'...")

        try:
            if preserve_order:
                # Use search_after for preserving order
                return self._get_documents_search_after(
                    index_name, query, size, total_docs, fields
                )
            else:
                # Use standard scroll
                return self._get_documents_standard_scroll(
                    index_name, query, scroll_time, size, total_docs
                )

        except Exception as e:
            print(f"❌ Error fetching documents: {str(e)}")
            raise

    def _get_documents_standard_scroll(self, index_name, query, scroll_time, size, total_docs):
        """Get documents using standard scroll API"""
        documents = []

        # Use helpers.scan for automatic scroll handling
        scroll_generator = scan(
            self.es,
            index=index_name,
            query=query,
            scroll=scroll_time,
            size=size,
            preserve_order=False
        )

        # Process with progress bar
        with tqdm(total=total_docs, desc=f"Fetching from {index_name}", unit="doc") as pbar:
            for hit in scroll_generator:
                # Add metadata
                doc = hit['_source']
                doc['_id'] = hit['_id']
                doc['_index'] = hit['_index']
                documents.append(doc)
                pbar.update(1)

        print(f"✅ Successfully retrieved {len(documents):,} documents")
        return documents

    def _get_documents_search_after(self, index_name, query, size, total_docs, fields):
        """Get documents using search_after for ordered pagination"""
        documents = []
        search_after = None
        has_more = True

        # Initial sort field (use @timestamp or _doc for natural order)
        sort_field = "@timestamp" if "@timestamp" in self.get_index_mapping(index_name) else "_doc"

        query['sort'] = [{sort_field: "asc"}]

        with tqdm(total=total_docs, desc=f"Fetching from {index_name}", unit="doc") as pbar:
            while has_more:
                # Prepare search body
                search_body = query.copy()

                if search_after:
                    search_body['search_after'] = search_after

                # Execute search
                response = self.es.search(
                    index=index_name,
                    body=search_body,
                    size=size,
                    sort=f"{sort_field}:asc"
                )

                hits = response['hits']['hits']

                if not hits:
                    has_more = False
                    break

                # Process hits
                for hit in hits:
                    doc = hit['_source']
                    doc['_id'] = hit['_id']
                    doc['_index'] = hit['_index']
                    documents.append(doc)
                    pbar.update(1)

                # Get last sort value for next page
                if hits:
                    search_after = hits[-1]['sort']

                # Check if we have all documents
                if len(hits) < size:
                    has_more = False

        print(f"✅ Successfully retrieved {len(documents):,} documents with preserved order")
        return documents

    def get_index_mapping(self, index_name):
        """Get field mapping for an index"""
        mapping = self.es.indices.get_mapping(index=index_name)
        fields = list(mapping[index_name]['mappings']['properties'].keys())
        return fields

    def export_to_file(self, documents, output_format='json', filename=None):
        """
        Export documents to file

        Args:
            documents: List of documents
            output_format: 'json', 'csv', or 'jsonl'
            filename: Output filename (auto-generated if None)
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"elasticsearch_export_{timestamp}.{output_format}"

        print(f"💾 Exporting {len(documents):,} documents to {filename}...")

        if output_format == 'json':
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False)

        elif output_format == 'jsonl':
            with open(filename, 'w', encoding='utf-8') as f:
                for doc in documents:
                    f.write(json.dumps(doc, ensure_ascii=False) + '\n')

        elif output_format == 'csv':
            if documents:
                df = pd.DataFrame(documents)
                df.to_csv(filename, index=False, encoding='utf-8')
            else:
                print("⚠️ No documents to export")
                return

        print(f"✅ Exported to {filename}")
        return filename

    def stream_to_file(self, index_name, output_file, query=None, batch_size=1000):
        """
        Stream documents directly to file (memory efficient for very large exports)

        Args:
            index_name: Index name
            output_file: Output file path
            query: Elasticsearch query
            batch_size: Batch size
        """
        if query is None:
            query = {"query": {"match_all": {}}}

        total_docs = self.get_total_count(index_name)

        print(f"📝 Streaming {total_docs:,} documents to {output_file}...")

        with open(output_file, 'w', encoding='utf-8') as f:
            # Write initial bracket for JSON array
            f.write('[')

            first_doc = True

            # Use scan generator
            scroll_generator = scan(
                self.es,
                index=index_name,
                query=query,
                scroll="10m",
                size=batch_size,
                preserve_order=False
            )

            with tqdm(total=total_docs, desc="Streaming", unit="doc") as pbar:
                for hit in scroll_generator:
                    doc = hit['_source']
                    doc['_id'] = hit['_id']
                    doc['_index'] = hit['_index']

                    # Add comma separator if not first document
                    if not first_doc:
                        f.write(',\n')
                    else:
                        first_doc = False

                    # Write document
                    f.write(json.dumps(doc, ensure_ascii=False))
                    pbar.update(1)

            # Close JSON array
            f.write(']')

        print(f"✅ Streamed {total_docs:,} documents to {output_file}")

    def close(self):
        """Close Elasticsearch connection"""
        self.es.transport.close()
        print("🔌 Connection closed")


def example_memory_efficient(scheme, host, port):
    """Memory-efficient streaming example"""
    exporter = ElasticsearchDataExporter(scheme=scheme, host=host, port=port)

    try:
        # Stream directly to file (uses minimal memory)
        exporter.stream_to_file(
            index_name="very_large_index",
            output_file="exported_data.json",
            batch_size=5000  # Larger batch for faster streaming
        )
    finally:
        exporter.close()


def example_multiple_indices(scheme, host, port):
    """Export from multiple indices"""
    exporter = ElasticsearchDataExporter(scheme, host, port)

    try:
        indices = ["index_2024_*", "logs_*"]  # Using patterns
        all_documents = []

        for index_pattern in indices:
            matching_indices = exporter.get_indices(pattern=index_pattern)

            for index_name in matching_indices:
                print(f"\n📂 Processing index: {index_name}")
                docs = exporter.get_all_documents_scroll(
                    index_name=index_name,
                    size=1000
                )
                all_documents.extend(docs)

        print(f"\n📦 Total documents from all indices: {len(all_documents):,}")
        exporter.export_to_file(all_documents, output_format='jsonl')

    finally:
        exporter.close()


def example_with_resume(scheme, host, port):
    """Example with resume capability"""
    exporter = ElasticsearchDataExporter(scheme=scheme, host=host, port=port)

    try:
        # Create a checkpoint file for resuming
        checkpoint_file = "checkpoint.json"
        last_id = None

        # Load checkpoint if exists
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                last_id = checkpoint.get('last_id')
                print(f"🔄 Resuming from ID: {last_id}")
        except FileNotFoundError:
            print("🆕 Starting fresh export")

        # Build query with search_after for resumable export
        query = {"query": {"match_all": {}}}
        sort_field = "_id"  # Use _id for consistent ordering

        documents = exporter.get_all_documents_scroll(
            index_name="my_index",
            query=query,
            preserve_order=True,
            size=2000
        )

        # Save checkpoint
        if documents:
            last_id = documents[-1]['_id']
            with open(checkpoint_file, 'w') as f:
                json.dump({'last_id': last_id}, f)

        exporter.export_to_file(documents, output_format='jsonl')

    finally:
        exporter.close()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Elasticsearch data')
    parser.add_argument('--scheme', default='http', help='Elasticsearch scheme (http or https)')
    parser.add_argument('--host', default='indexer', help='Elasticsearch host')
    parser.add_argument('--port', default='9200', help='Elasticsearch port')
    parser.add_argument('--index', required=True, help='Index name (supports wildcards)')
    parser.add_argument('--username', help='Username for authentication')
    parser.add_argument('--password', help='Password for authentication')
    parser.add_argument('--output', default='output.json', help='Output file')
    parser.add_argument('--format', choices=['json', 'jsonl', 'csv'], default='json')
    parser.add_argument('--size', type=int, default=1000, help='Batch size')
    parser.add_argument('--fields', help='Comma-separated list of fields to export')

    args = parser.parse_args()

    # Setup authentication if provided
    auth = (args.username, args.password) if args.username and args.password else None

    # Initialize exporter
    exporter = ElasticsearchDataExporter(
        scheme=args.scheme,
        host=args.host,
        port=int(args.port)
    )

    try:
        # Parse fields if provided
        fields = args.fields.split(',') if args.fields else None

        # Build query with field filtering
        query = {"query": {"match_all": {}}}
        if fields:
            query["_source"] = fields

        # Fetch documents
        documents = exporter.get_all_documents_scroll(
            index_name=args.index,
            query=query,
            size=args.size
        )

        # Export
        exporter.export_to_file(
            documents=documents,
            output_format=args.format,
            filename=args.output
        )

        print(f"\n🎉 Export completed successfully!")
        print(f"   Index: {args.index}")
        print(f"   Documents: {len(documents):,}")
        print(f"   Output: {args.output}")

    except Exception as e:
        print(f"\n❌ Export failed: {str(e)}")
        traceback.print_exc()

    finally:
        exporter.close()