<?php
/**
 * @link http://www.mercadoweb.com/
 * @copyright Copyright (c) 2016 MercadoWeb Ventures BV
 */

namespace vendor\mercadoweb\activecassandra;

use vendor\duoshuo;
use yii\base\Component;
use yii\base\InvalidConfigException;
use Yii;

class Connection extends Component
{
	const EVENT_AFTER_OPEN = 'afterOpen';
	/*
		Public's
	*/
	public $dsn;
	public $options = [];
	public $driverOptions = [];
	public $manager;
	public $typeMap = [];
	public $enableLogging = true;
	public $enableProfiling = true;
	/*
		Private's
	*/
	private $_defaultDatabaseName;
	private $_databases = [];
	private $_queryBuilder = 'yii\activecassandra\QueryBuilder';
	private $_logBuilder = 'yii\activecassandra\LogBuilder';
	/*
		defaultDatabaseName
	*/
	public function setDefaultDatabaseName($name)
    {
        $this->_defaultDatabaseName = $name;
    }
	
	public function getDefaultDatabaseName()
    {
        if ($this->_defaultDatabaseName === null) {
            if (preg_match('/^cassandra:\\/\\/.+\\/([^?&]+)/s', $this->dsn, $matches)) {
                $this->_defaultDatabaseName = $matches[1];
            } else {
                throw new InvalidConfigException("Unable to determine default database name from dsn.");
            }
        }

        return $this->_defaultDatabaseName;
    }
	/*
		QueryBuilder
	*/
	public function getQueryBuilder()
    {
        if (!is_object($this->_queryBuilder)) {
            $this->_queryBuilder = Yii::createObject($this->_queryBuilder, [$this]);
        }
        return $this->_queryBuilder;
    }
	
	public function setQueryBuilder($queryBuilder)
    {
        $this->_queryBuilder = $queryBuilder;
    }
	/*
		LogBuilder
	*/
	public function getLogBuilder()
    {
        if (!is_object($this->_logBuilder)) {
            $this->_logBuilder = Yii::createObject($this->_logBuilder);
        }
        return $this->_logBuilder;
    }
	
	public function setLogBuilder($logBuilder)
    {
        $this->_logBuilder = $logBuilder;
    }
	/*
		getDatabase
	*/
	public function getDatabase($name = null, $refresh = false)
    {
        if ($name === null) {
            $name = $this->getDefaultDatabaseName();
        }
        if ($refresh || !array_key_exists($name, $this->_databases)) {
            $this->_databases[$name] = $this->selectDatabase($name);
        }

        return $this->_databases[$name];
    }
	/*
		selectDatabase
	*/
	protected function selectDatabase($name)
    {
        return Yii::createObject([
            'class' => 'yii\activecassandra\Database',
            'name' => $name,
            'connection' => $this,
        ]);
    }
	/*
		getCollection
	*/
	public function getCollection($name, $refresh = false)
    {
        if (is_array($name)) {
            list ($dbName, $collectionName) = $name;
            return $this->getDatabase($dbName)->getCollection($collectionName, $refresh);
        } else {
            return $this->getDatabase()->getCollection($name, $refresh);
        }
    }
	/*
		getFileCollection
	*/
	public function getFileCollection($prefix = 'fs', $refresh = false)
    {
        if (is_array($prefix)) {
            list ($dbName, $collectionPrefix) = $prefix;
            if (!isset($collectionPrefix)) {
                $collectionPrefix = 'fs';
            }

            return $this->getDatabase($dbName)->getFileCollection($collectionPrefix, $refresh);
        } else {
            return $this->getDatabase()->getFileCollection($prefix, $refresh);
        }
    }
	/*
		getIsActive
	*/
	 public function getIsActive()
    {
        return is_object($this->manager) && $this->manager->getServers() !== [];
    }
	/*
		Open Connection
	*/
	public function open()
    {
        if ($this->manager === null) {
            if (empty($this->dsn)) {
                throw new InvalidConfigException($this->className() . '::dsn cannot be empty.');
            }
            $token = 'Opening Cassandra connection: ' . $this->dsn;
            try {
                Yii::trace($token, __METHOD__);
                Yii::beginProfile($token, __METHOD__);
                $options = $this->options;

                $this->manager = new Manager($this->dsn, $options, $this->driverOptions);
                $this->manager->selectServer($this->manager->getReadPreference());

                $this->initConnection();
                Yii::endProfile($token, __METHOD__);
            } catch (\Exception $e) {
                Yii::endProfile($token, __METHOD__);
                throw new Exception($e->getMessage(), (int) $e->getCode(), $e);
            }

            $this->typeMap = array_merge(
                $this->typeMap,
                [
                    'root' => 'array',
                    'document' => 'array'
                ]
            );
        }
    }
	/*
		Close Connection
	*/
	public function close()
    {
        if ($this->manager !== null) {
            Yii::trace('Closing Cassandra connection: ' . $this->dsn, __METHOD__);
            $this->manager = null;
            foreach ($this->_databases as $database) {
                $database->clearCollections();
            }
            $this->_databases = [];
        }
    }
	/*
		initConnection
	*/
	protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }
	/*
		createCommand
	*/
	public function createCommand($document = [], $databaseName = null)
    {
        return new Command([
            'db' => $this,
            'databaseName' => $databaseName,
            'document' => $document,
        ]);
    }
}

?>
