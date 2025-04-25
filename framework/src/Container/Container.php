<?php 

namespace Raj\Framework\Container;


use Psr\Container\ContainerInterface;
use Raj\Framework\Container\ContainerException;


class Container implements ContainerInterface{

    private array $services = [];


    public function get(string $id){

        return new $this->services[$id];
    }

    public function has(string $id):bool{

        //  TODO : Implement has()  method.
        return false;
    }

    public function add(string $id, string|object $concrete=null){

        if(null === $concrete){

            if(!class_exists($id)){
                throw new ContainerException("Services $id could not be found");
            }

            $concrete = $id;
        }

        $this->services[$id] = $concrete;

    }


}