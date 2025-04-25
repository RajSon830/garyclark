<?php 

namespace Raj\Framework\Tests;
use Raj\Framework\Tests\DependencyClass;

class DependentClass{

    public function __construct(private DependencyClass $dependency){

    }

    public function getDependency():DependencyClass{
        return $this->dependency;
    }

}