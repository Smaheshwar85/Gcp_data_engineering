import ConsumerOrders._

object ConsumerMonths extends java.io.Serializable{
  def main(args: Array[String]): Unit = {

    //select any number  from 1 to 12 for month selection
    var selectedMonth ="1"//arg(0)

     //it will return the index position
    def  month_index[A](nums: List[A], n: Int ): A = {
     if (n < 0) throw new IllegalArgumentException("Nth position is less than 1!")
     if (n > nums.length) throw new NoSuchElementException("Nth position greater than list length!")
     nums(n)
   }

    val month=selectedMonth.toInt
    val months= List("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan","Feb")
    monthly_data_selection(month_index(months, month-1),month_index(months, month+1),month_index(months, month+2))


    }







}
