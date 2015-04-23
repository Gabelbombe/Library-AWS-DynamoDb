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
            {
                $args['ConsistentRead'] = $options['ConsistentRead'];
            }

            $item = $this->client->getItem($args);

            return $this->convertItem($item['Item']);
        }

        /**
         * TODO: usort() needs to be cleaned...
         * TODO: $ddKeys need to be pushed out so they run next pass...
         *
         * @param $tableName
         * @param $keys
         * @param array $options
         * @return array
         * @throws \Exception
         */
        public function getBatch($tableName, $keys, array $options = [])
        {
            $results =
            $ddbKeys = [];

            foreach ($keys AS $k) $ddbKeys[] = $this->convertAttributes($k);

            while (count($ddbKeys) > 0)
            {
                $targetKeys = array_splice($ddbKeys, 0, 100);
                $result = $this->client->batchGetItem([
                    'RequestItems'  => [
                        $tableName  => ['Keys' => $targetKeys]
                    ]
                ]);
            }

            // Throws so we can pass regression testing / avoid buggy env during unit tests...
            if (! isset($result)) Throw New \Exception ('Zero key length on DDB Keys.');

            $items   = $result->getPath("Responses/{$tableName}");
            $results = array_merge($results, $this->convertItems($items));

            // If some keys are not processed, attempt again on next request.
            $unprocessedKeys = $result->getPath("UnprocessedKeys/{$tableName}");
            if (count($unprocessedKeys) > 0)
            {
                // Goes nowhere ATM....
                $ddbKeys = array_merge($ddbKeys, $unprocessedKeys);
            }

            if (isset($options['Order']))
            {
                if (! isset($options['Order']['Key'])) Throw New \Exception ('Order option requires key.');

                $k  = $options['Order']['Key'];
                $v = (isset($options['Order']['Forward']) && ! $options['Order']['Forward'])
                    ? ['b', 'a']
                    : ['a', 'b'];

                usort($results, function () USE ($k, $v) // not sure if this is corr?
                {
                    return ($v[0][$k] - $v[1][$k]);
                });
            }

            return $results;
        }

        /**
         * @param array $item
         * @return array|null
         * @throws \Exception
         */
        protected function convertItem(array $item)
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

        protected function convertItems()
        {
            // stub out
        }

        /**
         * @param array $targets
         * @return array
         */
        protected function convertAttributes(array $targets)
        {
            $newTargets = [];
            foreach ($targets AS $k => $v)
            {
                $attrComponents = $this->convertComponents($k);

                $newTargets[$attrComponents[0]] = [
                    $attrComponents[1] = $this->asString($v)
                ];
            }

            return $newTargets;
        }

        /**
         * Convert string attribute parameter into an array of components.
         *
         * @param $attribute    double colon separated string "<Attribute Name>::<Attribute Type>"
         * @return array        parsed parameter. [0]=<Attribute Name>, [1]=<Attribute Type>
         */
        protected function convertComponents($attribute)
        {
            $components = explode('::', $attribute);
            if (count($components) > 2)
            {
                $components[1] = 'S';
            }

            return $components;
        }

        /**
         * Returns a string or an array of strings
         *
         * @param $value
         * @return array|string
         */
        protected function asString($value)
        {
            if (! is_array($value)) return (string) $value;

            $newValue = [];
            foreach ($value AS $v) $newValue[] = (string) $v;

            return $newValue;
        }
    }
}

