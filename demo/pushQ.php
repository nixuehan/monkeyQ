<?php
require 'monkeyQ.php';

$monkey = \MonkeyQ\MonkeyQ::factory('127.0.0.1',9394);
$test = $monkey->setQueueName('timeline');


//数据入队列
$result = $test->message(json_encode(['name'=>'逆雪寒','blog'=>'http://naer.me']))
	 		   ->push();

var_dump($result);

//返回：
// array(6) {
//   ["success"]=>
//   bool(true)
//   ["QueueName"]=>
//   string(4) "test"
//   ["Id"]=>
//   string(10) "test_00001"
//   ["Body"]=>
//   string(55) "{"name":"\u9006\u96ea\u5bd2","blog":"http:\/\/naer.me"}"
//   ["DelaySeconds"]=>
//   string(0) ""
//   ["error"]=>
//   string(0) ""
// }