<?php
require 'monkeyQ.php';

$monkey = \MonkeyQ\MonkeyQ::factory('182.92.242.149',9394);
$delayTest = $monkey->setQueueName('wewewe');
var_dump($delayTest->deleteQueue());

