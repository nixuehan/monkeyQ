<?php
require 'monkeyQ.php';

$monkey = \MonkeyQ\MonkeyQ::factory('3.3.3.3',9394);
$delayTest = $monkey->setQueueName('wewewe');
var_dump($delayTest->deleteQueue());

