<?php
require 'monkey.php';

$monkey = \Monkey\Q::factory('182.92.242.149',9394);
// $test = $monkey->setQueueName('wewewe');

//更新普通消息队列
// $result = $test->setVisibilityTimeout(60) //设置
//    			   ->setMessageRetentionPeriod(76400)
// 			   ->update();

// var_dump($result);


//更新延迟消息队列
$delayTest = $monkey->setQueueName('asdfasdfsd');
$result = $delayTest->setVisibilityTimeout(30)
   			   ->setMessageRetentionPeriod(76400)
   			   ->setDelaySeconds(2)
			   ->update();

var_dump($result);


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

// $xxoo->deleteQueue();
// $xxoo->setWaitSeconds(30);

// while(true){
// 	$message = $xxoo->pop();
// 	var_dump($message);
// 	if($message){
// 		$xxoo->ID($message['Id'])->deleteMessage();
// 	}
// }



// for($i=1;$i<100;$i++){
// 	var_dump($xxoo->ID('sdwe_'.$i)
// 	   ->body("asdfsdfqwerqwdfasdfasd")
// 	   ->push());	
// }
