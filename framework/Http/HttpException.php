<?php 


namespace Raj\Framework\Http;

use Exception;

class HttpException extends Exception{

    private int $statusCode = 400;

    public function setStatusCode(int $statusCode){
        $this->statusCode=$statusCode;
    }

    public function getStatusCode(){
        return $this->statusCode;
    }

}