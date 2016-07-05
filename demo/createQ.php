<?php
require 'monkeyQ.php';

$monkey = \MonkeyQ\MonkeyQ::factory('127.0.0.1',9394);

//创建普通消息队列
$q = $monkey->setQueueName('timeline');
$result = $q->setVisibilityTimeout(5) //设置
			   ->create();

var_dump($result);

//设置延迟消息队列
// $delayTest = $monkey->setQueueName('wewewe');

// $result = $delayTest->setMessageRetentionPeriod(76400)
//    			   		->setDelaySeconds(10)
// 			   		->create();

// var_dump($result);


// array(6) {
//   ["success"]=>
//   bool(true)
//   ["QueueName"]=>
//   string(4) "test"
//   ["VisibilityTimeout"]=>
//   string(2) "30"
//   ["MessageRetentionPeriod"]=>
//   string(5) "76400"
//   ["DelaySeconds"]=>
//   string(1) "0"
//   ["error"]=>
//   string(0) ""
// }

