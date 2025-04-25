<?php 

namespace Raj\Framework\Http;

class Response{

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

}