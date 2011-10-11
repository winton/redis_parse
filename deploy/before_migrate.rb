if node['instance_role'].include?('util')
  run "cd #{latest_release} && make"
  run "chmod +x #{latest_release}/redis_parse"
end