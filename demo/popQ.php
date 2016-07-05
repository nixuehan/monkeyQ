<?php
require 'MonkeyQ.php';

$monkey = \MonkeyQ\MonkeyQ::factory('127.0.0.1',9394);
$test = $monkey->setQueueName('timeline');
$test->setWaitSeconds(5); //设置long polling 阻塞时间.默认：30秒


//轮训队列
while(true){
	$message = $test->pop();
	if($message->OK){
		var_dump($message->value()); //获取信息
		// var_dump($message->deleteMessage());//删除此信息
		var_dump($message->setVisibilityTime(0));//重新放入队列，设置变active时间
	}
}

//返回，然后阻塞
// array(4) {
//   ["success"]=>
//   bool(true)
//   ["Id"]=>
//   string(10) "test_00001"
//   ["Body"]=>
//   string(55) "{"name":"\u9006\u96ea\u5bd2","blog":"http:\/\/naer.me"}"
//   ["error"]=>
//   string(0) ""
// }a
