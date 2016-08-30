<?php
/**
 * @link http://www.mercadoweb.com/
 * @copyright Copyright (c) 2016 MercadoWeb Ventures BV
 */

namespace mercadoweb/cassandra

use mercadoweb\base\Component;
use mercadoweb\db\QueryInterface;
use mercadoweb\db\QueryTrait;
use mercadoweb;

class Query extends Component implements QueryInterface
{
	use QueryTrait;
	
	public $select = [];
	public $from;
	public $options = [];
	
	
	
} 

?>