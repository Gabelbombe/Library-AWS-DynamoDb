<?php
Namespace Wrappers
{
    require dirname(dirname(__DIR__)) . '/vendor/autoload.php';

    USE Aws\DynamoDb\DynamoDbClient;
    USE Aws\DynamoDb\Exception\ConditionalCheckFailedException;

    Class DynamoDb
    {
        protected $client;

        /**
         * Kickstart factory method to create new Amazon DynamoDB
         * client using an array of configuration options.
         *
         * @param $args
         * @return void
         * @link http://docs.aws.amazon.com/aws-sdk-php/v2/guide/configuration.html#client-configuration-options
         */
        public function __construct($args)
        {
            $this->client = DynamoDbClient::factory($args);
        }

        /**
         * @param $tableName
         * @param $key
         * @param array $options
         */
        public function get($tableName, $key, array $options = [])
        {
            $args = [
                'TableName' => $tableName,
                'Key'       => $this->convertAttributes($key),
            ];

            if (isset($options['ConsistentRead']))

                $args['ConsistentRead'] = $options['ConsistentRead'];

            $item = $this->client->getItem($args);

            return $this->convertItem($item['Item']);
        }

        protected function convertItem($item)
        {
            // stub out
        }

        protected function convertAttributes($targets)
        {
            // stub out
        }
    }
}

