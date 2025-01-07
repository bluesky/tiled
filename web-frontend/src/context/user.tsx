import React from 'react';

// // required to compare function by reference when context changes
// export const userUpdateFunctionTemplate = () => {}

export const userObjectContext = {
    user: "Ford Prefect",
    setUser: (user: string) => {console.log( user)}
  }


const UserContext = React.createContext(userObjectContext);

export default UserContext;
