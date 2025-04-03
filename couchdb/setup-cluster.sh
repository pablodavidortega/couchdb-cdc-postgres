#!/bin/bash
set -e
exec docker-entrypoint.sh "$@"

# Wait for CouchDB to be available
until curl -s http://localhost:5984/_up; do
  echo "Waiting for CouchDB to start..."
  sleep 2
done

echo "CouchDB is up. Configuring as a single-node cluster..."

# Set up CouchDB as a single-node cluster
curl -X PUT "http://admin:password@localhost:5984/_node/_local/_config/admins/admin" -d '"password"'
curl -X PUT "http://admin:password@localhost:5984/_node/_local/_config/couchdb/uuid" -d '"my-cluster-uuid"'
curl -X POST "http://admin:password@localhost:5984/_cluster_setup" \
     -H "Content-Type: application/json" \
     -d '{"action": "enable_single_node", "bind_address": "0.0.0.0", "username": "admin", "password": "password"}'

echo "CouchDB single-node cluster setup complete."

