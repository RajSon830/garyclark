<?php 

namespace Raj\Framework\Http;

class Response{

    public const HTTP_INTERNET_SERVER_ERROR = 500;

    public function __construct(private ?string $content='',
    private int $status=200,
    private array $headers=[]){

        // must be set before sending content
        // so best create an instantiation like here
        http_response_code($this->status);

    }

    public function send(){
        echo $this->content;
    }

    public function setContent(?string $content):void{
        $this->content = $content;
    }

    public function getStatus(){
        return $this->status;
    }

    public function getHeader(string $header){
        return $this->headers[$header];
    }

}