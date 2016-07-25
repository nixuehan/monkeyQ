<?php
namespace MonkeyQ;
/**
 * author 逆雪寒
 * version 0.7.1
 */
class MonkeyQ {

	private static $instance = NULL;
	private $host = '';

	private function __construct($host,$port){ 
		$this->host($host,$port);
	}
	 
	public function __clone(){
		trigger_error('Clone is not allow!',E_USER_ERROR);
	}

	protected function decode($data) {
		return @json_decode($data,true);
	}

	private function host($host,$port) {
		$this->host = "http://" . $host . ":" . $port."/";
	}

	public static function  factory($host = 'localhost',$port = 9394) {
		if(is_null(self::$instance)) {
			self::$instance = new self($host,$port);
		}
		return self::$instance;
	}

	protected function request($do,$parameter = '',$method = 'GET') {
		$query = '';
		$ch = curl_init(); 
		$query = http_build_query($parameter);

		if($method != 'GET') {
			curl_setopt($ch, CURLOPT_POSTFIELDS,$query);
			curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
			curl_setopt($ch, CURLOPT_URL, $this->host.$do);
		}else{
			$query = "?" . $query;
			curl_setopt($ch, CURLOPT_URL, $this->host.$do.$query);
		}
		curl_setopt($ch, CURLOPT_TIMEOUT,$this->waitSeconds);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER,true);

		$data = curl_exec($ch);
		if(curl_errno($ch)){ 
			return false;
		}
		curl_close($ch);
		return $data;
	}

	private $queueName = '';
	private $visibilityTimeout = 10;
	private $messageRetentionPeriod = 0;
	private $delaySeconds = 0;

	/**
	 * 设置队列名
	 *
	 * @param string $queueName 队列名
	 */
	public function setQueueName($queueName){
		$this->queueName = $queueName;
		return $this;
	}

	/**
	 * 设置消息可见时间
	 *
	 * @param int $visibilityTimeout 单位秒
	 */
	public function setVisibilityTimeout($visibilityTimeout){
		$this->visibilityTimeout = $visibilityTimeout;
		return $this;
	}

	/**
	 * 设置队列最大存储时间
	 *
	 * @param int $messageRetentionPeriod 单位秒
	 */
	public function setMessageRetentionPeriod($messageRetentionPeriod) {
		$this->messageRetentionPeriod = $messageRetentionPeriod;
		return $this;
	}

	/**
	 * 设置队列延迟时间
	 *
	 * @param int $delaySeconds 单位秒
	 */
	public function setDelaySeconds($delaySeconds){
		$this->delaySeconds = $delaySeconds;
		return $this;
	}

	/**
	 * 创建队列
	 */
	public function create() {
		if($this->queueName == ""){
			return false;
		}

		$result = $this->request('createQueue',[
			'QueueName' => $this->queueName,
			'VisibilityTimeout' => $this->visibilityTimeout,
			'MessageRetentionPeriod' => $this->messageRetentionPeriod,
			'DelaySeconds' => $this->delaySeconds
		],'POST');

		return $this->decode($result);
	}

	/**
	 * 更新队列设置
	 */
	public function update() {
		if($this->queueName == ""){
			return false;
		}

		$result = $this->request('updateQueue',[
			'QueueName' => $this->queueName,
			'VisibilityTimeout' => $this->visibilityTimeout,
			'MessageRetentionPeriod' => $this->messageRetentionPeriod,
			'DelaySeconds' => $this->delaySeconds
		],'POST');

		return $this->decode($result);
	}

	private $body = '';

	/**
	 * 消息主体
	 * @param string $body  消息内容
	 */
	public function message($body){
		$this->body = $body;
		return $this;
	}

	/**
	 * 发布消息
	 */
	public function push() {
		if($this->queueName == '' || $this->body == ''){
			return false;
		}

		$result = $this->request('push',[
			'queueName' => $this->queueName,
			'body' => $this->body
		],'POST');

		return $this->decode($result);
	}

	private $waitSeconds = 30; //默认30秒

	/**
	 * long polling时间 
	 * @param int $second  单位秒
	 */
	public function setWaitSeconds($second) {
		$this->waitSeconds = $second;
	}

	/**
	 * long polling
	 */
	public function pop() {
		if($this->queueName == ''){
			return false;
		}

		$item = $this->request('pop',[
			'queueName' => $this->queueName,
			'waitSeconds' => $this->waitSeconds
		],'GET');


		$message = Message::instance();
		$message->monkeyQ = $this;
		$message->in($this->decode($item));
		return $message;
	}

	/**
	 * long polling 守护
	 */
	public function watch(Array $fn) {

		if(!is_callable($fn['success']) || !is_callable($fn['fail'])){
			return false;
		}
		while(true){
			$message = $this->pop();
            if($message->OK){
       			$success = $fn['success'];
                $success($message);
            }else{
            	$fail = $fn['fail'];
                $fail($message);
            }
		}
	}

	/**
	 * 长轮训后获得到消息，可以进行：删除消息 
	 */
	public function deleteMessage() {
		if($this->queueName == '' || $this->body == ''){
			return false;
		}
		$result = $this->request('delMessage',[
			'queueName' => $this->queueName,
			'body' => $this->body
		],'POST');

		return $this->decode($result);
	}

	/**
	 * 长轮训后获得到消息，可以进行：重入消息队列
	 */
	public function setVisibilityTime($visibilityTime) {

		if($this->queueName == '' || $this->body == ''){
			return false;
		}

		$result =  $this->request('setVisibilityTime',[
			'queueName' => $this->queueName,
			'body' => $this->body,
			'visibilityTime' => (int)$visibilityTime
		],'POST');

		return $this->decode($result);
	}

	/**
	 * 删除队列
	 */
	public function deleteQueue() {
		if($this->queueName == ''){
			return false;
		}

		$result = $this->request('delQueue',[
			'queueName' => $this->queueName
		],'POST');

		return $this->decode($result);
	}
}

class Message{

	public $OK = false;
	public $queueName = '';
	public $monkeyQ = NULL;
	private $message = NULL;
	private static $instance = NULL;

	private function __construct(){  }
	 
	public function __clone(){
		trigger_error('Clone is not allow!',E_USER_ERROR);
	}

	public static function instance() {
		if(is_null(self::$instance)) {
			self::$instance = new self();
		}
		return self::$instance;
	}

	public function setVisibilityTime($visibilityTime = 0){
		return $this->monkeyQ->setVisibilityTime($visibilityTime);
	}

	public function deleteMessage(){
		return $this->monkeyQ->deleteMessage();
	}

	public function value(){
		return $this->message;
	}

	public function in($message){
		if(!$message){
			$this->OK = false;
		}else{
			$this->message = $message;
			if($this->message['success']){
				$this->monkeyQ->message($this->message['body']);
				$this->OK = true;
			}else{
				$this->OK = false;
			}
		}
	}
}



