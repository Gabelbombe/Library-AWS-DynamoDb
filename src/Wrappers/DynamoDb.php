<?php
/**
 * src/Wrappers/DynamoDb.php
 *
 * Wraps AWS DynamoDB methods via interface
 *
 * Copyright (C) 2015 Jd Daniel <dodomeki@gmail.com>
 *
 * LICENSE: This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://opensource.org/licenses/gpl-license.php>;.
 *
 * @package Wrappers\DynamoDb
 * @author  Jd Daniel :: Ehime <dodomeki@gmail.com>
 * @link    http://linkedin.com/in/ehinmeprefecture
 * @source  http://github.com/ehime/DynamoDB-Wrapper
 */
Namespace Wrappers
{
    require dirname(dirname(__DIR__)) . '/vendor/autoload.php';

    USE Aws\DynamoDb\DynamoDbClient;
    USE Aws\DynamoDb\Exception\ConditionalCheckFailedException;
    USE Aws\DynamoDb\Exception\DynamoDbException;

    /**
     * Interface WrapperInterface
     * @package Wrappers
     */
    Interface WrapperInterface
    {
        // Gets
        public function get         ($tableName, $key,         array $options);
        public function getBatch    ($tableName, array $keys,  array $options);

        // Puts
        public function put         ($tableName, $item,        array $expected);
        public function putBatch    ($tableName, $items);

        // Queries
        public function query       ($tableName, $conditions,  array $options);

        // Deletes
        public function delete      ($tableName, $key);
        public function deleteBatch ($tableName, $keys);

        // Table Manipulation
        public function createTable ($tableName, $hashKey,       $rangeKey,      array $options);
        public function deleteTable ($tableName);
        public function emptyTable  ($tableName);

        // Misc
        public function count       ($tableName, $conditions,  array $options);
        public function scan        ($tableName, $filter,      $limit);
    }

    /**
     * Class DynamoDb
     * @package Wrappers
     */
    Class DynamoDb Implements WrapperInterface
    {
        protected $client;

        /**
         * Kickstart factory method to create new Amazon DynamoDB client using an array of configuration options.
         * @link http://docs.aws.amazon.com/aws-sdk-php/v2/guide/configuration.html#client-configuration-options
         *
         * @param $args
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
         * @return array|null
         * @throws \Exception

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
         * Wrapper for BatchGetItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_BatchGetItem.html
         *
         * TODO: usort() needs to be cleaned...
         * TODO: $ddKeys need to be pushed out so they run next pass...
         *
         * @param $tableName
         * @param $keys
         * @param array $options
         * @return array
         * @throws \Exception
         */
        public function getBatch($tableName, array $keys, array $options = [])
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
                /* @ignore */ // Goes nowhere ATM....
                $ddbKeys = array_merge($ddbKeys, $unprocessedKeys);
            }


            /* Proposed fix for empty ddbKeys going nowhere...
             *
             * if (! empty($this->ddbKeys)) { $ddbKeys = $this->ddbKeys; $this->ddbKeys = null; }
             */


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
         * Wrapper for Query
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_Query.html
         *
         * @param $tableName
         * @param $keyConditions
         * @param array $options
         * @return array|null
         */
        public function query($tableName, $keyConditions, array $options = [])
        {
            $args = [
                'TableName'         => $tableName,
                'KeyConditions'     => $keyConditions,
                'ScanIndexForward'  => true,
                'Limit'             => 100,
            ];

            if (isset($options['ConsistentRead']))    $args['ConsistentRead']    = $options['ConsistentRead'];
            if (isset($options['IndexName']))         $args['IndexName']         = $options['IndexName'];
            if (isset($options['Limit']))             $args['Limit']             = $options['Limit'] + 0;
            if (isset($options['ExclusiveStartKey'])) $args['ExclusiveStartKey'] = $this->convertAttributes(
                $options['ExclusiveStartKey']
            );

            return $this->convertItems($this->client->query($args));
        }

        /**
         * Wrapper for Scan
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
         *
         * @param $tableName
         * @param $filter
         * @param null $limit
         * @return array|null
         */
        public function scan($tableName, $filter, $limit = null)
        {
            $scanFilter = (! empty($filter))
                ? $this->convertConditions($filter)
                : null;

            $items = $this->client->getIterator('Scan', [
                'TableName'     => $tableName,
                'ScanFilter'    => $scanFilter,
                'Limit'         => $limit,
            ]);

            return $this->convertItems($items);
        }

        /**
         * Wrapper for Query with `SELECT` param as `COUNT`
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
         *
         * @param $tableName
         * @param $keyConditions
         * @param array $options
         * @return integer|null
         */
        public function count($tableName, $keyConditions, array $options = [])
        {
            $args = [
                'TableName'         => $tableName,
                'KeyConditions'     => $keyConditions,
                'Select'            => 'COUNT',
            ];

            if (isset($options['IndexName'])) $args['IndexName'] = $options['IndexName'];

            return $this->client->query($args) ['Count'];
        }

        /**
         * Wrapper for PutItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_PutItem.html
         *
         * @param $tableName
         * @param $item
         * @param array $expected
         * @return bool
         * @throws ConditionalCheckFailedException
         */
        public function put($tableName, $item, array $expected = [])
        {
            $args = [
                'TableName'         => $tableName,
                'Item'              => $this->convertItem($item),
            ];

            if (! empty($expected)) $item['Expected'] = $expected;

            // Put and catch exception with Dynamo's ConditionalCheckFailed
            try
            {
                $this->client->putItem($args);
            }

            catch (ConditionalCheckFailedException $ccf)
            {
                return false;
            }

            return true;
        }

        /**
         * Wrapper for BatchWriteItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_BatchWriteItem.html
         *
         * @param $tableName
         * @param $items
         * @return mixed
         */
        public function putBatch($tableName, $items)
        {
            return $this->writeBatch('PutRequest', $tableName, $items);
        }

        /**
         * Wrapper for UpdateItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_UpdateItem.html
         *
         * @param $tableName
         * @param $key
         * @param $update
         * @param array $expected
         * @return array|null
         * @throws ConditionalCheckFailedException|\LogicException
         */
        public function update($tableName, $key, $update, array $expected = [])
        {
            $args = [
                'TableName'         => $tableName,
                'Key'               => $key,
                'AttributeUpdates'  => $this->convertUpdateAttributes($update),
                'ReturnValues'      => 'UPDATE_NEW',
            ];

            if (! empty($expected)) $item['Expected'] = $expected;

            // Put and catch exception with Dynamo's ConditionalCheckFailed
            try
            {
                $this->client->updateItem($args);
            }

            catch (ConditionalCheckFailedException $ccf)
            {
                return null;
            }

            if (! isset($item) || empty($item)) Throw New \LogicException ('Item was never instantiated.');

            return $this->convertItem($item['Attributes']);
        }

        /**
         * Wrapper for DeleteItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_DeleteItem.html
         *
         * @param $tableName
         * @param $key
         * @return array|null
         * @throws \Exception
         */
        public function delete($tableName, $key)
        {
            $args = [
                'TableName'         => $tableName,
                'Key'               => $this->convertAttributes($key),
                'ReturnValues'      => 'ALL_OLD',
            ];

            $result = $this->client->deleteItem($args);

            return $this->convertItem($result['Attributes']);
        }

        /**
         * Wrapper for BatchWriteItem
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_BatchWriteItem.html
         *
         * @param $tableName
         * @param $keys
         * @return bool
         */
        public function deleteBatch($tableName, $keys)
        {
            return $this->writeBatch('DeleteRequest', $tableName, $keys);
        }

        /**
         * Wrapper for CreateTable
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_BatchWriteItem.html
         *
         * @param $tableName
         * @param $hashKey
         * @param null $rangeKey
         * @param array $options
         * @throws \LogicException
         */
        public function createTable($tableName, $hashKey, $rangeKey = null, array $options = null)
        {
            $attributeDefinitions =
            $keySchema            = [];

            // HashKey
            $hashKeyComponents = $this->convertComponents($hashKey);
            $hashKeyName = $hashKeyComponents[0];
            $hashKeyType = $hashKeyComponents[1];
            $attributeDefinitions[] = [
                [ 'AttributeName' => $hashKeyName, 'AttributeType' => $hashKeyType ]
            ];
            $keySchema[] = [
                [ 'AttributeName' => $hashKeyName, 'KeyType' => 'HASH' ]

            ];

            // RangeKey
            if (isset($rangeKey))
            {
                $rangeKeyComponent = $this->convertComponents($rangeKey);
                $rangeKeyName = $rangeKeyComponent[0];
                $rangeKeyType = $rangeKeyComponent[1];
                $attributeDefinitions[] = [
                    [ 'AttributeName' => $rangeKeyName, 'AttributeType' => $rangeKeyType ]
                ];
                $keySchema[] = [
                    [ 'AttributeName' => $rangeKeyName, 'KeyType' => 'RANGE' ]

                ];
            }

            // Generate arguments
            $args = [
                'TableName'             => $tableName,
                'AttributeDefinitions'  => $attributeDefinitions,
                'KeySchema'             => $keySchema,
                'ProvisionedThroughput' => [
                    'ReadCapacityUnites'    => 1,
                    'WriteCapacityUnites'   => 1,
                ]
            ];

            // Set LocalSecondaryIndexes [if needed]
            if (isset($options['LocalSecondaryIndexes']))
            {
                foreach ($options['LocalSecondaryIndexes'] AS $lsi)
                {
                    $localSecondaryIndexes[] = [
                        'IndexName' => "{$lsi['name']}Index", //concat
                        'KeySchema' => [
                            [ 'AttributeName' => $hashKeyName, 'KeyType' => 'HASH'  ],
                            [ 'AttributeName' => $lsi['name'], 'KeyType' => 'RANGE' ],
                        ],
                        'Projection' => [
                            'ProjectionType'  => $lsi['projection_type'],
                        ],
                    ];
                    $attributeDefinitions[] = [
                        'AttributeName' => $lsi['name'],
                        'AttributeType' => $lsi['type'],
                    ];
                }

                if (! isset($localSecondaryIndexes) || empty($localSecondaryIndexes)) Throw New \LogicException ('Local Secondary Index was never instantiated.');

                $args['LocalSecondaryINdexes'] = $localSecondaryIndexes;
                $args['AttributeDefinitions']  = $attributeDefinitions;
            }

            $this->client->createTable($args);
            $this->client->waitUntilTableExists(['TableName' => $tableName]);
        }

        /**
         * Wrapper for DeleteTable
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_DeleteTable.html
         *
         * @param $tableName
         */
        public function deleteTable($tableName)
        {
            $this->client->deleteTable(['TableName' => $tableName]);
            $this->client->waitUntilTableNotExists(['TableName' => $tableName]);
        }

        /**
         * Wrappers for `DescribeTable` and `DeleteItem`
         * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_DescribeTable.html
         *
         * @param $tableName
         */
        public function emptyTable($tableName)
        {
            // Gets table info
            $result    = $this->client->describeTable(['TableName' => $tableName]);
            $keySchema = $result['TableName']['KeySchema'];
            foreach ($keySchema AS $schema)
            {
                if ('HASH' === $schema['KeyType'])
                {
                    $hashKeyName = $schema['AttributeName'];
                }

                elseif ('RANGE' === $schema['KeyType'])
                {
                    $rangeKeyName = $schema['AttributeName'];
                }
            }

            // Remove items from our table
            $scan = $this->client->getIterator('Scan', ['TableName' => $tableName]);
            foreach ($scan AS $item)
            {
                if (! isset($hashKeyName) || empty($hashKeyName)) Throw New \LogicException ('Hash Key Name was never instantiated.');

                // Set the hash key
                $hashKeyType = array_key_exists('S', $item[$hashKeyName])
                    ? 'S'
                    : 'N';

                $key = [
                    $hashKeyType => $item[$hashKeyName][$hashKeyType],
                ];

                if (isset($rangeKeyName))
                {
                    $rangeKeyType = array_key_exists('S', $item[$rangeKeyName])
                        ? 'S'
                        : 'N';

                    $key[$rangeKeyName] = [
                        $rangeKeyType => $item[$rangeKeyName][$rangeKeyType],
                    ];
                }

                $this->client->deleteItem([
                    'TableName' => $tableName,
                    'Key'       => $key,
                ]);
            }
        }

        /**
         * @param $tableName
         * @return bool
         */
        protected function tableExists($tableName)
        {
            try
            {
                $result = $this->client->describeTable(['TableName' => $tableName]);
            }

            catch (DynamoDbException $ddbe)
            {
                // If you want to be specific you can use something like:
                // $ddbe->getAwsErrorCode() === ResourceNotFound etc,
                return false;
            }

            return true;
        }


        /**
         * Alias for convertItem()
         *
         * @param $items
         * @return array|null
         * @throws \Exception
         */
        protected function convertItems($items)
        {
            $converted = [];
            foreach ($items AS $item) $converted = $this->convertItem($item);
            return $converted;
        }

        /**
         * Implementation of BatchWriteItem
         * http://docs.aws.amazon.com/amazondynamodb/latest/APIReference//API_BatchWriteItem.html
         *
         * @param $requestType
         * @param $tableName
         * @param $items
         * @return bool
         */
        protected function writeBatch($requestType, $tableName, $items)
        {
            $entityKeyName = ('PutRequest' === $requestType)
                ? 'Item'
                : 'Key';

            $requests = [];
            foreach ($items AS $item)
            {
                $requests[] = [
                    $requestType => [
                        $entityKeyName => $this->convertAttributes($item)
                    ]
                ];
            }

            while (count($requests) > 0)
            {
                $targetRequests = array_splice($requests, 0, 25);

                $result = $this->client->batchWriteItem([
                    'RequestItems' => [
                        $tableName => $targetRequests,
                    ]
                ]);


            }

            return true;
        }

        /**
         * Converts item to a usable Dynamo data reference
         *
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
         * @param array $targets
         * @return array
         */
        protected function convertUpdateAttributes(array $targets)
        {
            $newTargets = [];
            foreach ($targets AS $k => $v)
            {
                $attrComponents = $this->convertComponents($k);
                $newTargets[$attrComponents[0]] = [
                    'Action'    => $v[0],
                    'Value'     => [
                        $attrComponents[1] => $this->asString($v[1])
                    ],
                ];
            }

            return $newTargets;
        }

        /**
         * @param $conditions
         * @return array
         * @throws \Exception
         */
        protected function convertConditions($conditions)
        {
            $ddbConditions = [];
            foreach ($conditions AS $k => $v)
            {
                // gets attribs name and type...
                $attrComponent = $this->convertComponents($k);
                $attrName      = $attrComponent[0];
                $attrType      = $attrComponent[1];

                // gets ComparisonOperator and its value...
                if (! is_array($v))
                {
                    $v = [
                        'EQ', $this->asString($v)
                    ];
                }

                $comparisonOperator = $v[0];

                $value = (count($v) > 1)
                    ? $v[1]
                    : null;

                // get AttributeValueList...
                if ('BETWEEN' === $v[0])
                {
                    if (2 !== count($value)) Throw New \Exception ('BETWEEN requires 2 values as an array.');
                    $attributeValueList = [
                        [ $attrType => $this->asString($value[0]) ],
                        [ $attrType => $this->asString($value[1]) ],
                    ];
                }

                elseif ('IN' === $v[0])
                {
                    $attributeValueList = [];
                    foreach ($value AS $av)
                    {
                        $attributeValueList[] = [ $attrType => $this->asString($av) ];
                    }
                }

                elseif (('NOT_NULL' || 'NULL') === $v[0])
                {
                    $attributeValueList = null;
                }

                else
                {
                    $attributeValueList = [
                        [ $attrType => $this->asString($v) ]
                    ];
                }

                // construct key conditions for Dynamo
                $ddbConditions[$attrName] = [
                    'AttributeValueList'    => $attributeValueList,
                    'ComparisonOperator'    => $comparisonOperator,
                ];
            }

            return $ddbConditions;
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

