package com.demo

class SecondOrder(first: Int, second: Int) extends Ordered[SecondOrder] with Serializable {
    override def compare(other: SecondOrder) : Int = {
        if (this.first != other.first) {
            this.first - other.first
        } else {
            this.second - other.second
        }
    }
}
