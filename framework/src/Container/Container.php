<?php 

namespace Raj\Framework\Container;


use Psr\Container\ContainerInterface;
use Raj\Framework\Container\ContainerException;


class Container implements ContainerInterface{

    private array $services = [];


    public function get(string $id){

        if(!$this->has($id)){

            if(!class_exists($id)){
                throw new ContainerException("Services $id could not be resolved");
            }

            $this->add($id);
        }

        $object = $this->resolve($this->services[$id]);

        return $object;
    }


    private function resolve($class){

        // 1) Instantiate a Reflection Class ( dump and check )

        $reflationClass  = new \ReflectionClass($class);

        // 2) Use Reflation to try to obtain a class constructor 
        $constructor = $reflationClass->getConstructor();

        // 3) If there is no constructor, simply instantiate
        if(null === $constructor){
            return $reflationClass->newInstance();
        }

        // 4) Get the constructor parameters
        $constructorParams = $constructor->getParameters();

        #dd($constructorParams);
        // 5) Obtain dependencies

        $classDepencies = $this->resolveClassDependencies($constructorParams);

        // 6) Instantiate with dependencies
        $service = $reflationClass->newInstanceArgs($classDepencies);

        // 7) Return the object
        return $service;
    }

    private function resolveClassDependencies(array $reflectionParameters): array{

        // Initialize empty dependencies array (required ny mew Instance Args )

        $classDependencies = [];

        // 2. Try to locate and instantiate each parameter
        /** @var \ReflectionParameter $parameter */
        foreach($reflectionParameters as $paramter){
            $serviceType = $paramter->getType();

            $service = $this->get($serviceType->getName());

            $classDependencies[] = $service;

        }

        return $classDependencies;
        // Get the parameter's ReflectionNamedType as $serviceType

        //Try to instantiate using $serviceType's name

        // Add the service to the classDependencies as Array

    }


    public function has(string $id):bool{

        //  TODO : Implement has()  method.


        return array_key_exists($id,$this->services);
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