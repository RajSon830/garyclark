<?php 

namespace Raj\Framework\Http;

class Request{

    public function __construct(
        private readonly array $get,
        private readonly array $post,
        private readonly array $cookie,
        private readonly array $files,
        private readonly array $server
    ){
    }


    public static function createFromGlobals(): static{
        return new static($_GET,$_POST,$_COOKIE,$_FILES,$_SERVER);
    }

    public function getRequestMethod(){
        return $this->server['REQUEST_METHOD'];
    }

    public function getRequestUri(){
        return $this->server['REQUEST_URI'];
    }

    public function getPathInfo():string{
        return strtok($this->server['REQUEST_URI'],'?');
    }

    public function getPostParams(string $key){
        return $this->post[$key];
    }

}