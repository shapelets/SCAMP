#include "PiecewiseSCAMP.h"
#include <vector>
#include <queue>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <boost/python.hpp>
#include "SCAMP.h"
#include "SCAMP.pb.h"
#include "common.h"

using namespace SCAMP;

bool WriteProfileToFile(const std::string &mp, const std::string &mpi,
                        google::protobuf::RepeatedField<uint64_t> p) {
   std::ofstream mp_out(mp);
   std::ofstream mpi_out(mpi);
   for (int i = 0; i < p.size(); ++i) {
    SCAMP::mp_entry e;
    e.ulong = p.Get(i);
    mp_out << std::setprecision(10) << e.floats[0] << std::endl;
    mpi_out << e.ints[1] + 1 << std::endl;
  }

   return true;
}
class PiecewiseMP {
public:
//  PiecewiseMP(uint32_t window_size, SCAMPPrecisionType precision_type, SCAMPProfileType profile_type) : devices({0,1}) {
  PiecewiseMP(uint32_t window_size, uint32_t precision_type, uint32_t profile_type) : devices({0,1}) {
    op_template.set_window(window_size);
    op_template.set_max_tile_size(1 << 20);
    op_template.set_has_b(true);
    op_template.set_distributed_start_row(-1);
    op_template.set_distributed_start_col(-1);
    op_template.set_distance_threshold(static_cast<double>(std::nan("nan")));
    op_template.set_computing_columns(true);
    op_template.set_computing_rows(true);
    op_template.mutable_profile_a()->set_type((SCAMPProfileType)profile_type);
    op_template.mutable_profile_b()->set_type((SCAMPProfileType)profile_type);
    op_template.set_precision_type((SCAMPPrecisionType)precision_type);
    op_template.set_profile_type((SCAMPProfileType)profile_type);
    op_template.set_keep_rows_separate(true);
    op_template.set_is_aligned(false);
  }
//  void add_data(const std::vector<double> &segment) {
    void add_data(boost::python::list& pylist) {
      vector<double> segment;
      for (int i = 0; i < len(pylist); ++i) {
        segment.push_back(boost::python::extract<double>(pylist[i])); 
        
      }
      std::cout << segment.size() << std::endl;
      google::protobuf::RepeatedField<double> data(segment.begin(), segment.end());
// segment + len);
      pending_timeseries.push(data);
  }
  void update_mp() {
        while (!pending_timeseries.empty()) {
          SCAMPArgs op(op_template);
          auto series = pending_timeseries.front();
          pending_timeseries.pop();
          mp.push_back(std::move(get_new_mp_segment(series.size() - op.window() + 1)));
          op.mutable_timeseries_a()->CopyFrom(series);
          op.mutable_profile_a()
            ->mutable_data()
            ->Add()
            ->mutable_uint64_value()
            ->mutable_value()->CopyFrom(mp.back());
          op.mutable_profile_b()->mutable_data()->Add();
          for (int i = 0; i < current_timeseries.size(); ++i) {
            //std::cout << "MP index = " << i << std::endl;
            //std::cout << "current_timeseries = " << current_timeseries[i].size() << std::endl;
            //std::cout << "old size = " << op.timeseries_b().size() << std::endl;
            op.mutable_timeseries_b()->CopyFrom(current_timeseries[i]);
            //std::cout << "new size = " << op.timeseries_b().size() << std::endl;
            //std::cout << "current profile size = " << mp[i].size() << std::endl;
            //std::cout << "old_profile_size = " << op.profile_b().data().Get(0).uint64_value().value().size() << std::endl;
            op.mutable_profile_b()
            ->mutable_data()->Mutable(0)->mutable_uint64_value()
            ->mutable_value()->CopyFrom(mp[i]);
            //std::cout << "new_profile_size = " << op.profile_b().data().Get(0).uint64_value().value().size() << std::endl;
            //std::cout << "BEFORE" << std::endl;
            //std::cout << op.DebugString() << std::endl;
            do_SCAMP(&op, devices);
            //std::cout << "AFTER" << std::endl;
            //std::cout << op.DebugString() << std::endl;
            mp[i].CopyFrom(op.profile_b().data().Get(0).uint64_value().value());
          }
          
          mp.back().CopyFrom(op.profile_a().data().Get(0).uint64_value().value());
          current_timeseries.push_back(std::move(series));
          SCAMPArgs op_self(op_template);
          op_self.set_has_b(false);
          op_self.set_keep_rows_separate(false);
          op_self.mutable_timeseries_a()->CopyFrom(current_timeseries.back());
          op_self.mutable_profile_a()->mutable_data()->Add()->mutable_uint64_value()->mutable_value()->CopyFrom(mp.back());
          do_SCAMP(&op_self, devices);
          mp.back().CopyFrom(op_self.profile_a().data().Get(0).uint64_value().value());
        }
  }
  void write_mp() {
    int count = 0;
    const std::string mp_path = "piecewise_mp/mp_";
    const std::string mpi_path = "piecewise_mp/mpi_";
    for (auto elem : mp) {
      WriteProfileToFile(mp_path + std::to_string(count), mpi_path + std::to_string(count), elem);
      count++;
    }
  }

private:
  google::protobuf::RepeatedField<uint64_t> get_new_mp_segment(uint64_t len) {
      SCAMP::mp_entry e;
      e.floats[0] = std::numeric_limits<float>::lowest();
      e.ints[1] = -1u;
      vector<uint64_t> temp(len, e.ulong);
      google::protobuf::RepeatedField<uint64_t> data(temp.begin(),
                                                       temp.end());
      return data;
  }
  SCAMPArgs op_template;
  std::vector<google::protobuf::RepeatedField<double>> current_timeseries; 
  std::queue<google::protobuf::RepeatedField<double>> pending_timeseries; 
  std::vector<google::protobuf::RepeatedField<uint64_t>> mp;
  const std::vector<int> devices; 


};
BOOST_PYTHON_MODULE(pyscamp) {
    boost::python::class_<PiecewiseMP>("PiecewiseMP", boost::python::init<uint32_t,uint32_t,uint32_t>()).def("add_data", &PiecewiseMP::add_data).def("update_mp", &PiecewiseMP::update_mp).def("write_mp", &PiecewiseMP::write_mp);
}
