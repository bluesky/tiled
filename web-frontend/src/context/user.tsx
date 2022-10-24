import React from 'react';

// required to compare function by reference when context changes
export const userUpdateFunctionTemplate = () => {}

export const userObjectContext = {
    name: "Ford Prefect",
    updateUser: userUpdateFunctionTemplate,
  }


const UserContext = React.createContext(userObjectContext);

export default UserContext;
