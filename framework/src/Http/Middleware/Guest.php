<?php 

namespace Raj\Framework\Http\Middleware;

use Raj\Framework\Http\RedirectResponse;
use Raj\Framework\Http\Response;
use Raj\Framework\Http\Request;
use Raj\Framework\Session\SessionInterface;

class Guest implements MiddlewareInterface{

    private bool $authenticated = true;


    public function __construct(private SessionInterface $session){

    }

    public function process(Request $request,RequestHandlerInterface $requestHandlerInterface):Response{

        $this->session->start();

        if($this->session->has('auth_id')){
            return new RedirectResponse('/login');
        }

        return $requestHandlerInterface->handle($request);

    }
}