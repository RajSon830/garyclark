<?php 

namespace Raj\Framework\Http;

class Response{

    public const HTTP_INTERNET_SERVER_ERROR = 500;

    public function __construct(private ?string $content='',
    private int $status=200,
    private array $header=[]){

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

}