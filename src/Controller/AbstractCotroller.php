<?php 

namespace App\Controller;

use Psr\Container\ContainerInterface;
use Raj\Framework\Http\Request;
use Raj\Framework\Http\Response;

abstract class AbstractCotroller{

    protected ?ContainerInterface $container = null;
    protected Request $request;

    public function setContainer(ContainerInterface $container){
        $this->container = $container;
    }

    public function render(string $template,array $paramaters = [],?Response $response = null){

        $content = $this->container->get('twig')->render($template,$paramaters);

        $response ??= new Response();

        $response->setContent($content);

        return $response;
    }

    public function setRequest(Request $request){
        
        $this->request = $request;
    }


}
