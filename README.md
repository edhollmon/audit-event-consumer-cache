# Audit Event Consumer - Cache

This service is responsible for consuming audit events from Message Queue and sending them to cache.


## Getting Started

### Setup Elasticsearch

Install and run elasticsearch locally
<br>
<code>
   curl -fsSL https://elastic.co/start-local | sh
</code>
<br>

Copy API Key from terminal and create .env file in the root of the project with 
<br>
<code>
ELASTICSEARCH_API_KEY='your api key'
</code>


### Start Service Locally
Install dependancies
<br>
<code>npm install .</code>
<br>

Start the service
<br>
<code>npm run start </code>




