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
         * Kickstart factory method to create new Amazon DynamoDB client using an array of configuration options.
         * @link http://docs.aws.amazon.com/aws-sdk-php/v2/guide/configuration.html#client-configuration-options
         *
         * @param $args
         * @return void
         */
        public function __construct($args)
        {
            $this->client = DynamoDbClient::factory($args);
        }

        /**
         * Wrapper for GetItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_GetItem.html
         *
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

        /**
         * @param $item
         * @return array|null
         * @throws \Exception
         */
        protected function convertItem($item)
        {
            if (empty($item)) return null;

            $converted = [];
            foreach ($item AS $k => $v)
            {
                if      (isset($v['S']))  $converted[$k] = $v['S'];
                else if (isset($v['SS'])) $converted[$k] = $v['SS'];
                else if (isset($v['N']))  $converted[$k] = $v['N'];
                else if (isset($v['NS'])) $converted[$k] = $v['NS'];
                else if (isset($v['B']))  $converted[$k] = $v['B'];
                else if (isset($v['BS'])) $converted[$k] = $v['BS'];

                else Throw New \Exception ('Type not implemented');
            }

            return $converted;
        }

        protected function convertAttributes($targets)
        {
            // stub out
        }
    }
}

