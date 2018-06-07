docker run -d -p 7000:7000 -p 7001:7001 -p 7199:7199 -p 9042:9042 -p 9160:9160 cassandra

xcopy /Y .\projections-db-schema.sql tmp\
cd tmp
docker run -d -p 5432:5432 -e "POSTGRES_DB=BroadwayDataProjection" -v %cd%:/docker-entrypoint-initdb.d postgres

cd ..\..\kafka-docker
docker-compose -f docker-compose-single-broker.yml up
